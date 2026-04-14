import type {
  Frame,
  ParsedView,
  ParsedCluster,
  ParsedNode,
  QueueItem,
  Segment,
  Job,
  DeploymentGroup,
  Workload,
  ChipFree,
  ClusterChipFree,
  QuotaSummary,
  QueueEntry,
} from "./types";

export function parseFrame(frame: Frame | null): ParsedView | null {
  if (!frame) return null;

  const nodeAllocs = new Map<
    string,
    {
      cluster: string;
      capacity: number;
      chipType: string;
      used: number;
      allocations: { pod: string; phase: string; chips: number }[];
    }
  >();
  let totalCapacity = 0;
  let totalUsed = 0;

  for (const cluster of frame.clusters || []) {
    for (const node of cluster.nodes || []) {
      nodeAllocs.set(node.name, {
        cluster: cluster.name,
        capacity: node.chips || 0,
        chipType: node.chip_type || "",
        used: 0,
        allocations: [],
      });
      totalCapacity += node.chips || 0;
    }
  }

  const queue: QueueItem[] = [];
  for (const [podName, pod] of Object.entries(frame.pods || {})) {
    let queuedCount = 0;
    for (const replica of pod.statuses_by_replica || []) {
      if (replica.node) {
        if (!nodeAllocs.has(replica.node)) {
          nodeAllocs.set(replica.node, {
            cluster: "Unknown",
            capacity: 0,
            chipType: "",
            used: 0,
            allocations: [],
          });
        }
        const entry = nodeAllocs.get(replica.node)!;
        entry.allocations.push({
          pod: podName,
          phase: replica.phase,
          chips: pod.chips_per_replica || 1,
        });
        entry.used += pod.chips_per_replica || 1;
        totalUsed += pod.chips_per_replica || 1;
      } else if (replica.phase === "running" || replica.phase === "pending") {
        queuedCount += 1;
      }
    }
    if (queuedCount > 0) {
      queue.push({
        pod: podName,
        priority: pod.priority || 0,
        quota: pod.quota || "",
        chipType: pod.chip_type || "",
        chips: pod.chips_per_replica || 1,
        queued: queuedCount,
      });
    }
  }

  // Build gang index
  const gangSets = frame.gang_sets || [];
  const podToIdx = new Map<string, number>();
  const gangSetMembers = new Map<number, Set<string>>();
  let gangIdx = 1;
  for (const gang of gangSets
    .filter((v) => v.length > 0)
    .sort((a, b) => a[0].localeCompare(b[0]))) {
    const sorted = [...gang].sort();
    gangSetMembers.set(gangIdx, new Set(sorted));
    for (const pod of sorted) podToIdx.set(pod, gangIdx);
    gangIdx += 1;
  }
  for (const podName of Object.keys(frame.pods || {}).sort()) {
    if (!podToIdx.has(podName)) {
      podToIdx.set(podName, gangIdx);
      gangSetMembers.set(gangIdx, new Set([podName]));
      gangIdx += 1;
    }
  }

  queue.sort((a, b) => b.priority - a.priority);

  // Build cluster view with segments
  const clusterMap = new Map<
    string,
    {
      name: string;
      cluster: string;
      capacity: number;
      chipType: string;
      used: number;
      allocations: { pod: string; phase: string; chips: number }[];
    }[]
  >();
  for (const [nodeName, data] of nodeAllocs.entries()) {
    if (!clusterMap.has(data.cluster)) clusterMap.set(data.cluster, []);
    clusterMap.get(data.cluster)!.push({ name: nodeName, ...data });
  }

  const clusters: ParsedCluster[] = Array.from(clusterMap.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([clusterName, nodes]) => ({
      name: clusterName,
      nodes: nodes
        .sort((a, b) => a.name.localeCompare(b.name))
        .map((node): ParsedNode => {
          node.allocations.sort((a, b) => {
            const ga = podToIdx.get(a.pod) || Number.MAX_SAFE_INTEGER;
            const gb = podToIdx.get(b.pod) || Number.MAX_SAFE_INTEGER;
            if (ga !== gb) return ga - gb;
            return a.pod.localeCompare(b.pod);
          });

          const segments: Segment[] = [];
          for (const alloc of node.allocations) {
            const group = podToIdx.get(alloc.pod) || 0;
            if (
              !segments.length ||
              segments[segments.length - 1].gangIdx !== group
            ) {
              segments.push({
                gangIdx: group,
                multiPod: (gangSetMembers.get(group)?.size || 0) > 1,
                chipCount: alloc.chips,
                allocs: [alloc],
              });
            } else {
              const last = segments[segments.length - 1];
              last.allocs.push(alloc);
              last.chipCount += alloc.chips;
            }
          }

          return {
            name: node.name,
            chipType: node.chipType,
            capacity: node.capacity,
            used: node.used,
            segments,
            free: Math.max(0, node.capacity - node.used),
          };
        }),
    }));

  // ── Scale-aware aggregations ──
  const podCount = Object.keys(frame.pods || {}).length;
  const totalNodesCount = Array.from(nodeAllocs.values()).filter(
    (n) => n.capacity > 0,
  ).length;
  const isLargeScale = podCount > 500 || totalNodesCount > 200;

  const nodeToCluster = new Map<string, string>();
  for (const cluster of frame.clusters || [])
    for (const node of cluster.nodes || [])
      nodeToCluster.set(node.name, cluster.name);

  const poolKey = (cl: string, ct: string) => `${cl}\0${ct}`;
  const poolCap = new Map<string, number>();
  const poolUsed = new Map<string, number>();
  for (const cluster of frame.clusters || []) {
    for (const node of cluster.nodes) {
      const k = poolKey(cluster.name, node.chip_type || "");
      poolCap.set(k, (poolCap.get(k) || 0) + (node.chips || 0));
    }
  }
  for (const [, data] of nodeAllocs) {
    if (!data.capacity) continue;
    const k = poolKey(data.cluster, data.chipType);
    poolUsed.set(k, (poolUsed.get(k) || 0) + data.used);
  }

  // Classify pods
  const jobs: Job[] = [];
  const deployAgg = new Map<
    string,
    {
      quota: string;
      chipType: string;
      priority: number;
      running: number;
      pending: number;
      prefix: string;
      clusterCounts: Map<string, number>;
    }
  >();
  const quotaChipUsed = new Map<string, number>();

  for (const [podName, pod] of Object.entries(frame.pods || {})) {
    const replicas = (pod.statuses_by_replica || []).length;
    const cpr = pod.chips_per_replica || 1;
    const inGang =
      (gangSetMembers.get(podToIdx.get(podName) || 0)?.size || 0) > 1;
    const isJob = inGang;
    const quotaName = pod.quota || "default";
    const chipType = pod.chip_type || "";

    let placed = 0;
    let pending = 0;
    const podClusterCounts = new Map<string, number>();
    for (const r of pod.statuses_by_replica || []) {
      if (r.node) {
        placed++;
        const cl = nodeToCluster.get(r.node) || "Unknown";
        podClusterCounts.set(cl, (podClusterCounts.get(cl) || 0) + 1);
      } else if (r.phase === "running" || r.phase === "pending") {
        pending++;
      }
    }

    const qcKey = `${quotaName}\0${chipType}`;
    quotaChipUsed.set(qcKey, (quotaChipUsed.get(qcKey) || 0) + placed * cpr);

    if (isJob) {
      const gIdx = podToIdx.get(podName) ?? null;
      const gMembers = gIdx
        ? [...(gangSetMembers.get(gIdx) || [])].sort()
        : [podName];
      let status: Job["status"] = "pending";
      if (placed === replicas) status = "running";
      else if (placed > 0) status = "partial";
      if (
        replicas > 0 &&
        (pod.statuses_by_replica || []).every((r) => r.phase === "suspended")
      )
        status = "suspended";

      jobs.push({
        name: podName,
        totalGpus: replicas * cpr,
        replicas,
        chipsPerReplica: cpr,
        priority: pod.priority || 0,
        quota: quotaName,
        chipType,
        status,
        placedReplicas: placed,
        gangIdx: gIdx,
        gangMembers: gMembers,
        borrowing: false,
      });
    } else {
      let prefix = podName.includes("-")
        ? podName.substring(0, podName.lastIndexOf("-"))
        : podName;
      if (!prefix) prefix = podName;

      const dKey = `${prefix}\0${quotaName}\0${chipType}\0${pod.priority || 0}\0${cpr}`;
      const existing = deployAgg.get(dKey);
      if (existing) {
        existing.running += placed;
        existing.pending += pending;
        for (const [cl, count] of podClusterCounts)
          existing.clusterCounts.set(
            cl,
            (existing.clusterCounts.get(cl) || 0) + count,
          );
      } else {
        deployAgg.set(dKey, {
          quota: quotaName,
          chipType,
          priority: pod.priority || 0,
          running: placed,
          pending,
          prefix,
          clusterCounts: podClusterCounts,
        });
      }
    }
  }

  jobs.sort((a, b) => b.priority - a.priority || a.name.localeCompare(b.name));

  const deployments: DeploymentGroup[] = Array.from(deployAgg.entries()).map(
    ([dKey, d]) => {
      const cc: Record<string, number> = {};
      for (const [c, count] of d.clusterCounts) cc[c] = count;
      return {
        id: dKey,
        prefix: d.prefix,
        quota: d.quota,
        chipType: d.chipType,
        priority: d.priority,
        running: d.running,
        pending: d.pending,
        total: d.running + d.pending,
        clusterCounts: cc,
        borrowing: false,
      };
    },
  );
  deployments.sort((a, b) => b.total - a.total);

  const chipFreeMap = new Map<string, { free: number; total: number }>();
  for (const [k, cap] of poolCap) {
    const ct = k.split("\0")[1];
    const used = poolUsed.get(k) || 0;
    const entry = chipFreeMap.get(ct) || { free: 0, total: 0 };
    entry.total += cap;
    entry.free += Math.max(0, cap - used);
    chipFreeMap.set(ct, entry);
  }
  const chipFree: ChipFree[] = Array.from(chipFreeMap.entries())
    .map(([chipType, v]) => ({ chipType, ...v }))
    .sort((a, b) => b.total - a.total);

  const clusterChipFree: ClusterChipFree[] = [];
  for (const [k, cap] of poolCap) {
    const [cluster, chipType] = k.split("\0");
    const used = poolUsed.get(k) || 0;
    const free = Math.max(0, cap - used);
    clusterChipFree.push({ cluster, chipType, free, total: cap });
  }
  clusterChipFree.sort((a, b) => b.free - a.free);

  const quotaGuarantees = new Map<string, Map<string, number>>();
  for (const q of frame.quotas || []) {
    const chipMap = new Map<string, number>();
    for (const [, ctMap] of Object.entries(q.guarantees || {}))
      for (const [ct, chips] of Object.entries(ctMap))
        chipMap.set(ct, (chipMap.get(ct) || 0) + chips);
    quotaGuarantees.set(q.name, chipMap);
  }

  const quotaClusterGuarantee = new Map<string, number>();
  for (const q of frame.quotas || [])
    for (const [cl, ctMap] of Object.entries(q.guarantees || {}))
      for (const [ct, chips] of Object.entries(ctMap))
        quotaClusterGuarantee.set(`${q.name}\0${cl}\0${ct}`, chips);

  // Compute borrowing status for jobs and deployments
  for (const j of jobs) {
    const guaranteed = quotaGuarantees.get(j.quota)?.get(j.chipType) || 0;
    const used = quotaChipUsed.get(`${j.quota}\0${j.chipType}`) || 0;
    j.borrowing = guaranteed > 0 && used > guaranteed;
  }
  for (const d of deployments) {
    const guaranteed = quotaGuarantees.get(d.quota)?.get(d.chipType) || 0;
    const used = quotaChipUsed.get(`${d.quota}\0${d.chipType}`) || 0;
    d.borrowing = guaranteed > 0 && used > guaranteed;
  }

  const workloads: Workload[] = [
    ...jobs.map((j): Workload => ({ kind: "job", ...j })),
    ...deployments.map((d): Workload => ({ kind: "deployment", ...d })),
  ];
  workloads.sort((a, b) => {
    if (a.kind === "job" && b.kind === "job")
      return b.priority - a.priority || a.name.localeCompare(b.name);
    if (a.kind === "deployment" && b.kind === "deployment")
      return b.total - a.total;
    return a.kind === "job" ? -1 : 1;
  });

  const quotaNames = new Set<string>();
  for (const pod of Object.values(frame.pods || {}))
    quotaNames.add(pod.quota || "default");
  for (const q of frame.quotas || []) quotaNames.add(q.name);

  const quotaSummaries: QuotaSummary[] = [];
  for (const name of quotaNames) {
    const guar = quotaGuarantees.get(name) || new Map<string, number>();
    const chipTypes = new Set<string>();
    for (const ct of guar.keys()) chipTypes.add(ct);
    for (const [k] of quotaChipUsed) {
      const [q, ct] = k.split("\0");
      if (q === name) chipTypes.add(ct);
    }

    const chipBreakdown = Array.from(chipTypes)
      .map((ct) => ({
        chipType: ct,
        guaranteed: guar.get(ct) || 0,
        used: quotaChipUsed.get(`${name}\0${ct}`) || 0,
      }))
      .sort((a, b) => a.chipType.localeCompare(b.chipType));

    const qWorkloads: Workload[] = workloads.filter((w) => w.quota === name);

    const qClusterFree: ClusterChipFree[] = [];
    for (const ccf of clusterChipFree) {
      const hasGuarantee = quotaClusterGuarantee.has(
        `${name}\0${ccf.cluster}\0${ccf.chipType}`,
      );
      const hasUsage = qWorkloads.some((w) => w.chipType === ccf.chipType);
      if (hasGuarantee || hasUsage) qClusterFree.push(ccf);
    }

    quotaSummaries.push({
      name,
      chipBreakdown,
      workloads: qWorkloads,
      clusterFree: qClusterFree,
    });
  }
  quotaSummaries.sort((a, b) => a.name.localeCompare(b.name));

  const queueEntries: QueueEntry[] = [];
  for (const j of jobs) {
    if (j.status === "pending" || j.status === "partial") {
      queueEntries.push({
        kind: "job",
        name: j.name,
        priority: j.priority,
        quota: j.quota,
        chipType: j.chipType,
        chips: j.totalGpus,
        count: 1,
      });
    }
  }
  for (const d of deployments) {
    if (d.pending > 0) {
      queueEntries.push({
        kind: "deployment-rollup",
        name: d.id,
        priority: d.priority,
        quota: d.quota,
        chipType: d.chipType,
        chips: d.pending,
        count: d.pending,
      });
    }
  }
  queueEntries.sort((a, b) => b.priority - a.priority);
  if (queueEntries.length > 150) queueEntries.length = 150;

  return {
    clusters,
    queue,
    gangInfo: { podToIdx, gangSetMembers },
    totalNodes: totalNodesCount,
    utilization: totalCapacity
      ? Math.round((totalUsed / totalCapacity) * 100)
      : 0,
    isLargeScale,
    chipFree,
    clusterChipFree,
    quotaSummaries,
    jobs,
    deployments,
    workloads,
    queueEntries,
  };
}
