import { fetchJson } from "./api";
import type {
  Frame,
  ParsedView,
  ParsedCluster,
  ParsedNode,
  QueueItem,
  Segment,
  GangInfo,
  Job,
  DeploymentGroup,
  Workload,
  ChipFree,
  ClusterChipFree,
  QuotaSummary,
  QueueEntry,
} from "./types";

function parseFrame(frame: Frame | null): ParsedView | null {
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
    const isJob = replicas > 1 || cpr > 1 || inGang;
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
      });
    } else {
      let prefix = podName.includes("-")
        ? podName.substring(0, podName.lastIndexOf("-"))
        : podName;
      if (!prefix) prefix = podName;

      const dKey = `${quotaName}\0${chipType}\0${pod.priority || 0}\0${cpr}`;
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
      };
    },
  );
  deployments.sort((a, b) => b.total - a.total);

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

function computeHighlightCss(
  selectedPod: string | null,
  selectedGangIdx: number | null,
  gangSetMembers: Map<number, Set<string>>,
): string {
  let members: string[] = [];
  if (selectedGangIdx != null) {
    const set = gangSetMembers.get(selectedGangIdx);
    if (!set || !set.size) return "";
    members = [...set];
  } else if (selectedPod) {
    members = [selectedPod];
  } else {
    return "";
  }

  const selectors = members.map((pod) => {
    const safe = pod.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
    return {
      chip: `.chip[data-pod="${safe}"]`,
      queue: `.queue-item[data-pod="${safe}"]`,
    };
  });

  const chipMatches = selectors.map((s) => s.chip).join(", ");
  const queueMatches = selectors.map((s) => s.queue).join(", ");
  const chipExclusions = selectors.map((s) => `:not(${s.chip})`).join("");
  const queueExclusions = selectors.map((s) => `:not(${s.queue})`).join("");

  return `
    body.has-selection .chip[data-pod]${chipExclusions} { opacity: 0.16; filter: grayscale(1); }
    body.has-selection .queue-item[data-pod]${queueExclusions} { opacity: 0.3; }
    body.has-selection ${chipMatches} { transform: scale(1.2); box-shadow: 0 0 0 2px var(--bg), 0 0 0 4px var(--text); }
    body.has-selection ${queueMatches} { border-color: rgba(237, 246, 251, 0.5); background: rgba(22, 50, 67, 0.95); }
  `;
}

const LIVE_MAX_FRAMES = 100;

class SimState {
  frames = $state<Frame[]>([]);
  currentFrameIdx = $state(0);
  displayFrame = $state<Frame | null>(null);
  playing = $state(false);
  fps = $state(5);
  currentMode = $state<"none" | "replay" | "live">("none");
  currentSource = $state<"none" | "live" | "scenario" | "file">("none");
  currentScenarioName = $state("");
  currentSessionUrl = $state("");
  liveScheduler = $state("heuristic");
  replayRunSolver = $state(true);
  replaySolver = $state("heuristic");
  homeLiveScheduler = $state("heuristic");
  homeScenarioSolver = $state("heuristic");
  selectedPod = $state<string | null>(null);
  selectedGangIdx = $state<number | null>(null);
  selectedQuota = $state<string | null>(null);
  selectedWorkload = $state<string | null>(null);
  selectedChipType = $state<string | null>(null);
  globalSearch = $state("");
  solvedFrames = $state<Record<number, Frame>>({});
  frameBusy = $state(false);
  sliderDragging = $state(false);
  sliderValue = $state(0);
  homeVisible = $state(true);
  activeTab = $state<"queue" | "generator">("queue");
  helpOpen = $state(false);
  autoFollow = $state(true);
  connectionText = $state("Disconnected");
  connectionKind = $state("");
  errorMessage = $state("");
  errorVisible = $state(false);
  scenarios = $state<{ name: string; description: string }[]>([]);
  solvers = $state<{ name: string; ref: string }[]>([]);
  genConnected = $state(false);
  genRunning = $state(false);
  genFormDirty = $state(false);
  genForm = $state({
    seed: "7",
    arrival_rate: "0.15",
    burst_factor: "1.4",
    loop_interval_seconds: "5",
    priority_min: "30",
    priority_max: "99",
    replica_min: "1",
    replica_max: "2",
    runtime_min: "12",
    runtime_max: "40",
    gang_frequency: "0.08",
    replica_failure_rate: "0.03",
    node_failure_rate: "0.005",
    node_recovery_rate: "0.03",
    quota_weights: '{"training": 1, "research": 1, "inference": 1}',
    chip_weights: '{"H200": 1, "H100": 1, "A100": 1, "L40S": 0.7}',
    chips_weights: '{"1": 0.2, "2": 0.25, "4": 0.3, "8": 1}',
  });

  livePollTimer: ReturnType<typeof setInterval> | null = null;
  liveLastSeq = 0;
  frameRequestId = 0;
  queuedFrameIndex: number | null = null;
  sliderRequestedIdx = 0;
  sliderInputTimer: ReturnType<typeof setTimeout> | null = null;
  errorTimer: ReturnType<typeof setTimeout> | null = null;
  genPollTimer: ReturnType<typeof setInterval> | null = null;
  genPolling = false;
  playAnimId: number | null = null;

  parsedView = $derived.by(() => parseFrame(this.displayFrame));
  highlightCss = $derived.by(() =>
    computeHighlightCss(
      this.selectedPod,
      this.selectedGangIdx,
      this.parsedView?.gangInfo.gangSetMembers ?? new Map(),
    ),
  );
  hasSelection = $derived(
    this.selectedPod !== null || this.selectedGangIdx !== null,
  );
  counterText = $derived.by(() => {
    if (!this.frames.length) return "0 / 0";
    if (this.currentMode === "live")
      return `${this.currentFrameIdx + 1} / last ${this.frames.length}`;
    return `${this.currentFrameIdx + 1} / ${this.frames.length}`;
  });
  modePillText = $derived.by(() => {
    if (this.currentMode === "live") return `Live \u2022 ${this.liveScheduler}`;
    if (this.currentMode === "replay")
      return `Replay \u2022 ${this.replaySolver}`;
    return "No session";
  });
  snapshotMetaText = $derived.by(() => {
    const frame = this.displayFrame;
    if (!frame) return "No snapshot";
    if (this.currentMode === "live") {
      const time = frame.timestamp
        ? new Date(frame.timestamp).toLocaleTimeString()
        : "--";
      return `seq ${frame.seq} \u2022 ${time} \u2022 ${this.liveScheduler}`;
    }
    return `t=${this.currentFrameIdx} \u2022 ${this.replaySolver}`;
  });
  statRunningText = $derived.by(() => {
    const frame = this.displayFrame;
    if (!frame) return "--";
    if (this.currentMode === "live")
      return `${frame.summary?.running_jobs || 0} running`;
    const pv = this.parsedView;
    if (!pv) return "--";
    const placed = pv.clusters.reduce(
      (sum, c) =>
        sum +
        c.nodes.reduce(
          (ns, n) => ns + n.segments.reduce((ss, s) => ss + s.allocs.length, 0),
          0,
        ),
      0,
    );
    return `${placed} placed`;
  });
  statQueuedText = $derived.by(() => {
    const frame = this.displayFrame;
    if (!frame) return "--";
    if (this.currentMode === "live")
      return `${frame.summary?.queued_jobs || 0} queued`;
    const pv = this.parsedView;
    if (!pv) return "--";
    return `${pv.queue.reduce((s, q) => s + q.queued, 0)} queued`;
  });
  statUtilText = $derived.by(() => {
    const frame = this.displayFrame;
    if (!frame) return "--";
    if (this.currentMode === "live")
      return `${(frame.summary?.utilization_percent || 0).toFixed(2)}% util`;
    const pv = this.parsedView;
    if (!pv) return "--";
    return `${pv.utilization}% util`;
  });
  statSolverText = $derived.by(() => {
    const frame = this.displayFrame;
    if (!frame) return "--";
    if (this.currentMode === "live") return frame.solver_status || "--";
    const solved = this.replayRunSolver
      ? this.solvedFrames[this.currentFrameIdx]
      : null;
    return solved?.solver_status || "--";
  });
  statDurationText = $derived.by(() => {
    const frame = this.displayFrame;
    if (!frame) return "--";
    let dMs: number | undefined;
    if (this.currentMode === "live") {
      dMs = frame.solver_duration_ms;
    } else {
      const solved = this.replayRunSolver
        ? this.solvedFrames[this.currentFrameIdx]
        : null;
      dMs = solved?.solver_duration_ms;
    }
    return typeof dMs === "number" && Number.isFinite(dMs)
      ? `${Math.max(0, Math.round(dMs))}ms`
      : "--";
  });
  nodesSummaryText = $derived(
    this.parsedView ? `${this.parsedView.totalNodes} nodes` : "0 nodes",
  );
  utilSummaryText = $derived(
    this.parsedView ? `${this.parsedView.utilization}% util` : "0% util",
  );
  queueLabel = $derived.by(() => {
    const frame = this.displayFrame;
    if (!frame) return "";
    if (this.currentMode === "live") return `seq ${frame.seq || 0}`;
    return `t=${this.currentFrameIdx}`;
  });
  queueCaption = $derived.by(() => {
    if (!this.displayFrame) return "";
    return "Pods awaiting placement \u2014 input to the solver at this timestep";
  });
  clusterCaption = $derived.by(() => {
    if (!this.displayFrame) return "";
    if (this.currentMode === "live") {
      const frame = this.displayFrame;
      return `Cluster state at seq ${frame.seq || 0} \u2014 solver called with this state as input`;
    }
    return `Cluster state at t=${this.currentFrameIdx} \u2014 solver called with this state as input`;
  });

  // ── Methods ──

  selectQuota(name: string | null) {
    this.selectedQuota = this.selectedQuota === name ? null : name;
    this.selectedWorkload = null;
    this.selectedChipType = null;
  }

  selectWorkload(name: string | null) {
    this.selectedWorkload = this.selectedWorkload === name ? null : name;
    if (this.selectedWorkload && this.parsedView) {
      const job = this.parsedView.jobs.find(
        (j) => j.name === this.selectedWorkload,
      );
      if (job) {
        this.selectedQuota = job.quota;
        this.selectedChipType = null;
      } else {
        const dep = this.parsedView.deployments.find(
          (d) => d.id === this.selectedWorkload,
        );
        if (dep) {
          this.selectedQuota = dep.quota;
          this.selectedChipType = null;
        }
      }
    }
  }

  selectChipType(ct: string | null) {
    this.selectedChipType = this.selectedChipType === ct ? null : ct;
    this.selectedQuota = null;
    this.selectedWorkload = null;
  }

  clearSelection() {
    this.selectedPod = null;
    this.selectedGangIdx = null;
    this.selectedQuota = null;
    this.selectedWorkload = null;
    this.selectedChipType = null;
  }

  clearSmallScaleSelection() {
    this.selectedPod = null;
    this.selectedGangIdx = null;
  }

  jobHistory(jobName: string): { frame: number; status: string }[] {
    const events: { frame: number; status: string }[] = [];
    let lastStatus = "absent";
    for (let i = 0; i < this.frames.length; i++) {
      const pod = this.frames[i]?.pods?.[jobName];
      let status = "absent";
      if (pod) {
        const placed = (pod.statuses_by_replica || []).some((r) => r.node);
        const allSuspended =
          (pod.statuses_by_replica || []).length > 0 &&
          (pod.statuses_by_replica || []).every((r) => r.phase === "suspended");
        if (allSuspended) status = "suspended";
        else if (placed) status = "running";
        else status = "pending";
      }
      if (i === 0 || status !== lastStatus) {
        events.push({ frame: i, status });
        lastStatus = status;
      }
    }
    return events;
  }

  showError(message: string) {
    this.errorMessage = message;
    this.errorVisible = true;
    if (this.errorTimer) clearTimeout(this.errorTimer);
    this.errorTimer = setTimeout(() => (this.errorVisible = false), 5000);
  }

  clampFrameIndex(index: number): number {
    if (!this.frames.length) return 0;
    return Math.max(0, Math.min(index, this.frames.length - 1));
  }

  private _syncRouteTimer: ReturnType<typeof setTimeout> | null = null;

  routeState() {
    const params = new URLSearchParams();
    let path = "/";
    if (this.currentSource === "live") {
      path = "/live";
      params.set("scheduler", this.liveScheduler);
      if (this.frames.length) params.set("frame", String(this.currentFrameIdx));
    } else if (this.currentSource === "scenario" && this.currentScenarioName) {
      path = `/scenarios/${encodeURIComponent(this.currentScenarioName)}`;
      params.set("solver", this.replaySolver);
      if (this.frames.length) params.set("frame", String(this.currentFrameIdx));
    } else if (this.currentSource === "file") {
      path = "/replay";
      params.set("solver", this.replaySolver);
      params.set("run_solver", this.replayRunSolver ? "1" : "0");
      if (this.currentSessionUrl) params.set("session", this.currentSessionUrl);
      if (this.frames.length) params.set("frame", String(this.currentFrameIdx));
    }
    return { path, params };
  }

  syncRoute() {
    if (this._syncRouteTimer) return;
    this._syncRouteTimer = setTimeout(() => {
      this._syncRouteTimer = null;
      const url = new URL(window.location.href);
      const route = this.routeState();
      url.pathname = route.path;
      url.search = route.params.toString();
      window.history.replaceState({}, "", url);
    }, 200);
  }

  async loadScenarios() {
    const data = await fetchJson("/scenarios/index.json");
    this.scenarios = data;
  }
  async loadSolvers() {
    const data = await fetchJson("/api/solvers");
    this.solvers = data;
    this.replaySolver = "heuristic";
    this.liveScheduler = "heuristic";
    this.homeLiveScheduler = "heuristic";
    this.homeScenarioSolver = "heuristic";
  }
  disconnectLive() {
    if (this.livePollTimer) {
      clearInterval(this.livePollTimer);
      this.livePollTimer = null;
    }
    if (this.currentMode !== "live") {
      this.connectionText = "Disconnected";
      this.connectionKind = "";
    }
  }
  resetAppState() {
    this.frames = [];
    this.currentFrameIdx = 0;
    this.playing = false;
    this.solvedFrames = {};
    this.selectedPod = null;
    this.selectedGangIdx = null;
    this.frameBusy = false;
    this.queuedFrameIndex = null;
    this.sliderValue = 0;
    this.sliderRequestedIdx = 0;
    this.displayFrame = null;
  }
  openHome(updateRoute = true) {
    this.disconnectLive();
    this.currentMode = "none";
    this.currentSource = "none";
    this.currentScenarioName = "";
    this.currentSessionUrl = "";
    this.resetAppState();
    if (updateRoute) this.syncRoute();
    this.homeVisible = true;
    this.connectionText = "Disconnected";
    this.connectionKind = "";
  }
  initApp(mode: "replay" | "live") {
    this.currentMode = mode;
    this.homeVisible = false;
    this.solvedFrames = {};
    this.selectedPod = null;
    this.selectedGangIdx = null;
    this.currentFrameIdx = 0;
    this.sliderValue = 0;
    this.sliderRequestedIdx = 0;
    this.displayFrame = null;
    if (mode === "live" && !this.genPolling) this.startGenPolling();
    if (mode !== "live" && this.activeTab === "generator")
      this.activeTab = "queue";
  }
  async loadScenario(
    options: { name?: string; solver?: string; frame?: number } = {},
  ) {
    this.disconnectLive();
    const name = options.name || this.scenarios[0]?.name || "gang_scheduling";
    const solver = options.solver || this.homeScenarioSolver || "heuristic";
    const response = await fetch(
      `/scenarios/${encodeURIComponent(name)}.jsonl`,
    );
    if (!response.ok)
      throw new Error(`Failed to load scenario: ${response.statusText}`);
    const text = await response.text();
    const data = text
      .trim()
      .split("\n")
      .filter((line) => line)
      .map((line) => JSON.parse(line));
    this.frames = data;
    this.currentSource = "scenario";
    this.currentScenarioName = name;
    this.currentSessionUrl = "";
    this.replaySolver = solver;
    const podCount = Object.keys(data[0]?.pods || {}).length;
    this.replayRunSolver = podCount < 500;
    this.initApp("replay");
    this.syncRoute();
    await this.requestFrame(Number(options.frame ?? 0));
  }
  async parseText(
    text: string,
    routeParams: {
      solver?: string;
      runSolver?: boolean;
      frame?: number;
      session?: string;
    } = {},
  ) {
    const parsed: Frame[] = [];
    for (const line of text.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        parsed.push(JSON.parse(trimmed));
      } catch {
        continue;
      }
    }
    if (!parsed.length) throw new Error("No valid JSONL lines found.");
    this.disconnectLive();
    this.frames = parsed;
    this.currentSource = "file";
    this.currentScenarioName = "";
    this.currentSessionUrl = routeParams.session || "";
    this.replaySolver = routeParams.solver || this.replaySolver || "heuristic";
    this.replayRunSolver = routeParams.runSolver ?? true;
    this.initApp("replay");
    this.syncRoute();
    await this.requestFrame(Number(routeParams.frame ?? 0));
  }
  async loadUrl(
    url: string,
    options: { solver?: string; runSolver?: boolean; frame?: number } = {},
  ) {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    await this.parseText(await response.text(), { ...options, session: url });
  }
  handleFile(file: File) {
    const reader = new FileReader();
    reader.onload = async (event) => {
      try {
        await this.parseText(event.target!.result as string);
      } catch (error: any) {
        this.showError(error.message);
      }
    };
    reader.readAsText(file);
  }
  async bootstrapLive(frame: number | null = null) {
    const scheduler = this.liveScheduler;
    this.disconnectLive();
    this.currentSource = "live";
    this.currentScenarioName = "";
    this.currentSessionUrl = "";
    this.connectionText = "Connecting...";
    this.connectionKind = "";
    try {
      const latest = await fetchJson(
        `/state/latest-${encodeURIComponent(scheduler)}.json`,
      );
      this.frames = [latest];
      this.liveLastSeq = latest.seq || 0;
    } catch {
      this.frames = [];
      this.showError(`No live data yet for ${scheduler}.`);
    }
    this.initApp("live");
    this.syncRoute();
    const targetFrame =
      frame === null ? Math.max(this.frames.length - 1, 0) : Number(frame);
    await this.requestFrame(targetFrame);
    this.connectionText = "Connected";
    this.connectionKind = "live";
    this.livePollTimer = setInterval(() => this.pollLiveSnapshot(), 500);
  }
  async pollLiveSnapshot() {
    if (this.currentMode !== "live") return;
    try {
      const scheduler = encodeURIComponent(this.liveScheduler);
      const snap = await fetchJson(`/state/latest-${scheduler}.json`);
      const seq = snap?.seq || 0;
      if (seq > this.liveLastSeq) {
        this.liveLastSeq = seq;
        this.upsertLiveSnapshot(snap);
      }
    } catch {
      this.connectionText = "Reconnecting...";
      this.connectionKind = "error";
    }
  }
  upsertLiveSnapshot(snapshot: Frame) {
    if (
      this.currentMode !== "live" ||
      snapshot.scheduler !== this.liveScheduler
    )
      return;
    const existingIndex = this.frames.findIndex((f) => f.seq === snapshot.seq);
    if (existingIndex >= 0) {
      this.frames[existingIndex] = snapshot;
    } else {
      this.frames.push(snapshot);
      this.frames.sort((a, b) => (a.seq || 0) - (b.seq || 0));
    }
    if (this.frames.length > LIVE_MAX_FRAMES) {
      const excess = this.frames.length - LIVE_MAX_FRAMES;
      this.frames.splice(0, excess);
      this.currentFrameIdx = Math.max(0, this.currentFrameIdx - excess);
    }
    if (this.autoFollow || this.currentFrameIdx >= this.frames.length - 2) {
      this.requestFrame(this.frames.length - 1);
    } else {
      this.displayFrame = this.frames[this.currentFrameIdx];
      this.syncRoute();
    }
  }
  async solveFrame(frame: Frame) {
    const started = performance.now();
    const solver = encodeURIComponent(this.replaySolver.trim());
    const solved = await fetchJson(`/api/solve?solver=${solver}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(frame),
    });
    if (typeof solved.solver_duration_ms !== "number")
      solved.solver_duration_ms = Math.round(performance.now() - started);
    return solved;
  }
  buildWorldState(index: number): Frame {
    const raw = this.frames[index];
    if (index === 0 || !this.solvedFrames[index - 1]) return raw;
    const previous = this.solvedFrames[index - 1];
    const mergedPods: Record<string, any> = {};
    for (const [name, pod] of Object.entries(raw.pods || {}))
      mergedPods[name] = previous.pods?.[name] || pod;
    return { ...raw, pods: mergedPods };
  }
  async ensureSolvedUpTo(index: number, requestId: number): Promise<boolean> {
    for (let i = 0; i < index; i++) {
      if (requestId !== this.frameRequestId) return false;
      if (this.solvedFrames[i]) continue;
      const world = this.buildWorldState(i);
      const solved = await this.solveFrame(world);
      if (requestId !== this.frameRequestId) return false;
      this.solvedFrames[i] = {
        ...world,
        pods: solved.pods,
        solver_status: solved.solver_status || "ok",
        solver_duration_ms: solved.solver_duration_ms ?? null,
      };
    }
    return true;
  }
  async requestFrame(index: number) {
    if (!this.frames.length) return;
    const target = this.clampFrameIndex(Number(index));
    if (this.frameBusy) {
      this.queuedFrameIndex = target;
      this.frameRequestId += 1;
      return;
    }
    this.frameBusy = true;
    try {
      await this.setFrame(target);
    } finally {
      this.frameBusy = false;
      const queued = this.queuedFrameIndex;
      this.queuedFrameIndex = null;
      if (queued !== null && queued !== this.currentFrameIdx)
        this.requestFrame(queued);
    }
  }
  async setFrame(index: number) {
    if (!this.frames.length) return;
    const requestId = ++this.frameRequestId;
    this.currentFrameIdx = this.clampFrameIndex(index);
    if (!this.sliderDragging) {
      this.sliderValue = this.currentFrameIdx;
      this.sliderRequestedIdx = this.currentFrameIdx;
    }
    if (this.currentMode === "live") {
      const frame = this.frames[this.currentFrameIdx];
      if (requestId !== this.frameRequestId) return;
      this.displayFrame = frame;
      this.syncRoute();
      return;
    }
    const raw = this.frames[this.currentFrameIdx];
    if (!this.replayRunSolver) {
      if (requestId !== this.frameRequestId) return;
      this.displayFrame = raw;
      this.syncRoute();
      return;
    }
    let world: Frame = raw;
    try {
      if (
        this.currentFrameIdx > 0 &&
        !this.solvedFrames[this.currentFrameIdx - 1]
      ) {
        const caughtUp = await this.ensureSolvedUpTo(
          this.currentFrameIdx,
          requestId,
        );
        if (!caughtUp || requestId !== this.frameRequestId) return;
      }
      world = this.buildWorldState(this.currentFrameIdx);
      if (requestId !== this.frameRequestId) return;
      this.displayFrame = world;
      const solved = await this.solveFrame(world);
      if (requestId !== this.frameRequestId) return;
      this.solvedFrames[this.currentFrameIdx] = {
        ...world,
        pods: solved.pods,
        solver_status: solved.solver_status || "ok",
        solver_duration_ms: solved.solver_duration_ms ?? null,
      };
      this.solvedFrames = { ...this.solvedFrames };
    } catch (error: any) {
      if (requestId !== this.frameRequestId) return;
      this.showError(error.message);
    }
    this.syncRoute();
  }
  togglePlay() {
    if (!this.frames.length) return;
    this.playing = !this.playing;
    if (!this.playing) return;
    if (this.currentFrameIdx >= this.frames.length - 1) this.requestFrame(0);
    let last = performance.now();
    let advancing = false;
    const loop = (now: number) => {
      if (!this.playing) return;
      if (!advancing && now - last >= 1000 / this.fps) {
        last = now;
        if (this.currentFrameIdx >= this.frames.length - 1) {
          this.playing = false;
          return;
        }
        advancing = true;
        Promise.resolve(this.requestFrame(this.currentFrameIdx + 1)).finally(
          () => (advancing = false),
        );
      }
      requestAnimationFrame(loop);
    };
    requestAnimationFrame(loop);
  }
  stepPrev() {
    this.playing = false;
    this.requestFrame(this.currentFrameIdx - 1);
  }
  stepNext() {
    this.playing = false;
    this.requestFrame(this.currentFrameIdx + 1);
  }
  handleSliderInput(value: number) {
    this.sliderValue = value;
    this.sliderRequestedIdx = value;
    this.playing = false;
    if (this.sliderInputTimer) clearTimeout(this.sliderInputTimer);
    if (!this.frames.length) return;
    const delay =
      this.currentMode === "replay" && this.replayRunSolver ? 90 : 0;
    this.sliderInputTimer = setTimeout(() => this.requestFrame(value), delay);
  }
  handleSliderPointerDown() {
    this.sliderDragging = true;
  }
  handleSliderPointerUp() {
    if (!this.sliderDragging) return;
    this.sliderDragging = false;
    if (this.frames.length) this.sliderValue = this.sliderRequestedIdx;
  }
  handlePodClick(podName: string, gangGroupElement: Element | null) {
    const gangIdx = gangGroupElement
      ? Number(gangGroupElement.getAttribute("data-gang"))
      : (this.parsedView?.gangInfo.podToIdx.get(podName) ?? null);
    const gangMembers =
      gangIdx != null
        ? this.parsedView?.gangInfo.gangSetMembers.get(gangIdx)
        : null;
    if (gangMembers && gangMembers.size > 1) {
      this.selectedGangIdx = this.selectedGangIdx === gangIdx ? null : gangIdx;
      this.selectedPod = null;
    } else {
      this.selectedPod = this.selectedPod === podName ? null : podName;
      this.selectedGangIdx = null;
    }
  }
  onReplaySolverChange() {
    this.solvedFrames = {};
    if (
      this.currentMode === "replay" &&
      this.replayRunSolver &&
      this.frames.length
    )
      this.requestFrame(this.currentFrameIdx);
    else this.syncRoute();
  }
  onReplayRunSolverChange(checked: boolean) {
    this.replayRunSolver = checked;
    this.solvedFrames = {};
    if (this.currentMode === "replay" && this.frames.length)
      this.requestFrame(this.currentFrameIdx);
    else this.syncRoute();
  }
  onLiveSchedulerChange() {
    if (this.currentMode === "live")
      this.bootstrapLive().catch((e: any) => this.showError(e.message));
  }
  fillGenForm(config: any) {
    this.genForm = {
      seed: String(config.seed),
      arrival_rate: String(config.arrival_rate),
      burst_factor: String(config.burst_factor),
      loop_interval_seconds: String(config.loop_interval_seconds),
      priority_min: String(config.priority_min),
      priority_max: String(config.priority_max),
      replica_min: String(config.replica_min),
      replica_max: String(config.replica_max),
      runtime_min: String(config.runtime_min),
      runtime_max: String(config.runtime_max),
      gang_frequency: String(config.gang_frequency),
      replica_failure_rate: String(config.replica_failure_rate),
      node_failure_rate: String(config.node_failure_rate),
      node_recovery_rate: String(config.node_recovery_rate),
      quota_weights: JSON.stringify(config.quota_weights, null, 2),
      chip_weights: JSON.stringify(config.chip_weights, null, 2),
      chips_weights: JSON.stringify(config.chips_weights, null, 2),
    };
    this.genFormDirty = false;
  }
  genFormPayload() {
    return {
      seed: Number(this.genForm.seed),
      arrival_rate: Number(this.genForm.arrival_rate),
      burst_factor: Number(this.genForm.burst_factor),
      loop_interval_seconds: Number(this.genForm.loop_interval_seconds),
      priority_min: Number(this.genForm.priority_min),
      priority_max: Number(this.genForm.priority_max),
      replica_min: Number(this.genForm.replica_min),
      replica_max: Number(this.genForm.replica_max),
      runtime_min: Number(this.genForm.runtime_min),
      runtime_max: Number(this.genForm.runtime_max),
      gang_frequency: Number(this.genForm.gang_frequency),
      replica_failure_rate: Number(this.genForm.replica_failure_rate),
      node_failure_rate: Number(this.genForm.node_failure_rate),
      node_recovery_rate: Number(this.genForm.node_recovery_rate),
      quota_weights: JSON.parse(this.genForm.quota_weights),
      chip_weights: JSON.parse(this.genForm.chip_weights),
      chips_weights: JSON.parse(this.genForm.chips_weights),
    };
  }
  async genRefresh() {
    try {
      const config = await fetchJson("/state/config.json");
      this.genConnected = true;
      this.genRunning = config.running ?? true;
      if (!this.genFormDirty) this.fillGenForm(config);
    } catch {
      this.genConnected = false;
    }
  }
  startGenPolling() {
    if (this.genPolling) return;
    this.genPolling = true;
    this.genRefresh();
    this.genPollTimer = setInterval(() => {
      if (this.activeTab === "generator") this.genRefresh();
    }, 2000);
  }
  async genAction(fn: () => Promise<void>) {
    try {
      await fn();
    } catch (error: any) {
      this.showError(error.message);
    }
  }
  async genSetRunning(running: boolean) {
    await this.genAction(async () => {
      await fetchJson("/api/generator/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ running }),
      });
      await this.genRefresh();
    });
  }
  async genStart() {
    await this.genSetRunning(true);
  }
  async genPause() {
    await this.genSetRunning(false);
  }
  async genResume() {
    await this.genSetRunning(true);
  }
  async genSaveConfig() {
    await this.genAction(async () => {
      const payload = this.genFormPayload();
      await fetchJson("/api/generator/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      this.genFormDirty = false;
      await this.genRefresh();
    });
  }
  async initFromUrl() {
    await Promise.all([this.loadScenarios(), this.loadSolvers()]);
    const params = new URLSearchParams(window.location.search);
    const pathname = decodeURIComponent(
      window.location.pathname.replace(/\/+$/, "") || "/",
    );
    if (pathname === "/replay" && params.get("session")) {
      try {
        await this.loadUrl(params.get("session")!, {
          solver: params.get("solver") || "heuristic",
          runSolver: params.get("run_solver") === "1",
          frame: Number(params.get("frame") || 0),
        });
        return;
      } catch (error: any) {
        this.showError(error.message);
      }
    }
    if (pathname === "/live" || params.get("mode") === "live") {
      try {
        if (params.get("scheduler"))
          this.liveScheduler = params.get("scheduler")!;
        await this.bootstrapLive(Number(params.get("frame") || 0));
        return;
      } catch (error: any) {
        this.showError(error.message);
      }
    }
    if (
      pathname.startsWith("/scenarios/") ||
      params.get("mode") === "scenario"
    ) {
      const scenario = pathname.startsWith("/scenarios/")
        ? pathname.split("/").pop()!
        : params.get("scenario");
      if (scenario) {
        try {
          await this.loadScenario({
            name: scenario,
            solver: params.get("solver") || "heuristic",
            frame: Number(params.get("frame") || 0),
          });
          return;
        } catch (error: any) {
          this.showError(error.message);
        }
      }
    }
    if (params.get("session")) {
      try {
        await this.loadUrl(params.get("session")!, {
          solver: params.get("solver") || "heuristic",
          runSolver: params.get("run_solver") === "1",
          frame: Number(params.get("frame") || 0),
        });
        return;
      } catch (error: any) {
        this.showError(error.message);
      }
    }
  }
}

export const sim = new SimState();
