<script lang="ts">
  import { sim } from "../lib/state.svelte";
  import { chipColor } from "../lib/api";
  import type { Workload } from "../lib/types";

  let { workload }: { workload: Workload } = $props();

  function priDisplay(pri: number): {
    label: string;
    cls: string;
    bars: number;
  } {
    if (pri >= 50) return { label: "Fast lane", cls: "sv-pri-fast", bars: 4 };
    if (pri >= 40) return { label: "High", cls: "sv-pri-high", bars: 3 };
    if (pri >= 20) return { label: "Medium", cls: "sv-pri-med", bars: 2 };
    return { label: "Low", cls: "sv-pri-low", bars: 1 };
  }

  function chipFreeCount(ct: string): number {
    return sim.parsedView?.chipFree.find((c) => c.chipType === ct)?.free ?? 0;
  }

  function clusterForNode(nodeName: string): string | null {
    for (const c of sim.parsedView?.clusters ?? []) {
      if (c.nodes.some((n) => n.name === nodeName)) return c.name;
    }
    return null;
  }

  function eventTimeLabel(e: {
    seq: number | null;
    timestamp: string | null;
    frame: number;
  }): string {
    const parts: string[] = [];
    if (e.seq != null) parts.push(`seq ${e.seq}`);
    if (e.timestamp) {
      try {
        parts.push(new Date(e.timestamp).toLocaleTimeString());
      } catch {
        /* ignore bad timestamps */
      }
    }
    return parts.length ? parts.join(" · ") : `t=${e.frame}`;
  }

  const jobDetail = $derived(
    workload.kind === "job"
      ? (sim.parsedView?.jobs.find((j) => j.name === workload.name) ?? null)
      : null,
  );
  const deployDetail = $derived(
    workload.kind === "deployment"
      ? (sim.parsedView?.deployments.find((d) => d.id === workload.id) ?? null)
      : null,
  );

  const jobReplicasByCluster = $derived.by<
    { cluster: string; nodes: { node: string; count: number }[] }[]
  >(() => {
    if (!jobDetail || !sim.displayFrame?.pods) return [];
    const pod = sim.displayFrame.pods[jobDetail.name];
    if (!pod) return [];
    const clusterNodes = new Map<string, Map<string, number>>();
    for (const r of pod.statuses_by_replica || []) {
      const node = r.node ?? "unplaced";
      const cl = clusterForNode(node) || "unknown";
      if (!clusterNodes.has(cl)) clusterNodes.set(cl, new Map());
      const nodeMap = clusterNodes.get(cl)!;
      nodeMap.set(node, (nodeMap.get(node) || 0) + 1);
    }
    return Array.from(clusterNodes.entries())
      .map(([cluster, nodeMap]) => ({
        cluster,
        nodes: Array.from(nodeMap.entries())
          .map(([node, count]) => ({ node, count }))
          .sort((a, b) => b.count - a.count),
      }))
      .sort((a, b) => {
        const ac = a.nodes.reduce((s, n) => s + n.count, 0);
        const bc = b.nodes.reduce((s, n) => s + n.count, 0);
        return bc - ac;
      });
  });

  const queuePos = $derived.by(() => {
    const id = jobDetail?.name ?? deployDetail?.id;
    if (!id) return null;
    const entries = sim.parsedView?.queueEntries ?? [];
    const idx = entries.findIndex((e) => e.name === id);
    if (idx < 0) return null;
    const ct = jobDetail?.chipType || deployDetail?.chipType;
    const ahead = entries.slice(0, idx).filter((e) => e.chipType === ct);
    return {
      position: ahead.length + 1,
      chipsAhead: ahead.reduce((s, e) => s + e.chips, 0),
    };
  });

  const jobEvents = $derived(jobDetail ? sim.jobHistory(jobDetail.name) : []);
  const deployEvents = $derived.by(() => {
    if (!deployDetail) return [];
    const idParts = deployDetail.id.split("\0");
    const cpr = idParts.length >= 5 ? Number(idParts[4]) || 1 : 1;
    return sim.deploymentHistory(
      deployDetail.prefix,
      deployDetail.quota,
      deployDetail.chipType,
      deployDetail.priority,
      cpr,
    );
  });
</script>

{#if jobDetail}
  <div class="sv-card">
    <div class="sv-card-head">
      {jobDetail.name}
      <button class="sv-x" onclick={() => sim.selectWorkload(null)}
        >&times;</button
      >
    </div>
    <div class="sv-card-row">
      <span class="sv-tag {priDisplay(jobDetail.priority).cls}"
        >{priDisplay(jobDetail.priority).label}</span
      >
      <span class="sv-tag"
        >{jobDetail.replicas}&times;{jobDetail.chipsPerReplica}<span
          style="color:{chipColor(jobDetail.chipType)}"
          >&times;{jobDetail.chipType}</span
        ></span
      >
      <span class="sv-tag">{jobDetail.quota}</span>
      <span class="sv-tag">{jobDetail.status}</span>
    </div>
    {#if jobDetail.borrowing}
      <div class="sv-card-warn">
        Running on borrowed quota — at risk of preemption
      </div>
    {/if}
    {#if queuePos}
      <div class="sv-card-note">
        Pos {queuePos.position} &middot; {queuePos.chipsAhead.toLocaleString()}
        {jobDetail.chipType} ahead &middot;
        {chipFreeCount(jobDetail.chipType).toLocaleString()} free (need
        {jobDetail.totalGpus})
      </div>
    {/if}
    {#if jobDetail.gangMembers.length > 1}
      <div class="sv-card-row">
        <span class="sv-dim">Gang:</span>
        {#each jobDetail.gangMembers as m}
          <button
            class="sv-gang"
            class:self={m === jobDetail.name}
            onclick={() => sim.selectWorkload(m)}>{m}</button
          >
        {/each}
      </div>
    {/if}
    {#if jobReplicasByCluster.length}
      <div class="sv-placement">
        <span class="sv-dim">Placement for this job:</span>
        {#each jobReplicasByCluster as group}
          {@const total = group.nodes.reduce((s, n) => s + n.count, 0)}
          <button
            class="sv-cluster-link"
            title="{total} replica{total === 1 ? '' : 's'} on {group.cluster}"
            onclick={() => sim.selectCluster(group.cluster)}
          >
            {group.cluster} &mdash; {total} replica{total === 1 ? "" : "s"}
          </button>
        {/each}
      </div>
    {/if}
    {#if jobEvents.length}
      <div class="sv-events">
        <h5>History</h5>
        {#each jobEvents as ev}
          <button class="sv-event" onclick={() => sim.requestFrame(ev.frame)}>
            <span class="sv-event-time">{eventTimeLabel(ev)}</span>
            {ev.status}
          </button>
        {/each}
      </div>
    {/if}
  </div>
{:else if deployDetail}
  <div class="sv-card">
    <div class="sv-card-head">
      {deployDetail.prefix}
      <button class="sv-x" onclick={() => sim.selectWorkload(null)}
        >&times;</button
      >
    </div>
    <div class="sv-card-row">
      <span class="sv-tag {priDisplay(deployDetail.priority).cls}"
        >{priDisplay(deployDetail.priority).label}</span
      >
      <span class="sv-tag"
        >{deployDetail.running}/{deployDetail.total} pods</span
      >
      <span class="sv-tag" style="color:{chipColor(deployDetail.chipType)}"
        >{deployDetail.chipType}</span
      >
      <span class="sv-tag">{deployDetail.quota}</span>
    </div>
    {#if deployDetail.borrowing}
      <div class="sv-card-warn">
        Running on borrowed quota — at risk of preemption
      </div>
    {/if}
    <div class="sv-card-note">
      {deployDetail.running.toLocaleString()} running / {deployDetail.total.toLocaleString()}
      total
      {#if deployDetail.pending > 0}&middot; {deployDetail.pending.toLocaleString()}
        pending{/if}
    </div>
    {#if Object.keys(deployDetail.clusterCounts).length}
      <div class="sv-placement">
        {#each Object.entries(deployDetail.clusterCounts).sort((a, b) => b[1] - a[1]) as [cl, ct]}
          <button
            class="sv-node sv-node-link"
            title="{ct} pod{ct === 1 ? '' : 's'} on {cl}"
            onclick={() => sim.selectCluster(cl)}
            >{cl} &mdash; {ct} pod{ct === 1 ? "" : "s"}</button
          >
        {/each}
      </div>
    {/if}
    {#if deployEvents.length}
      <div class="sv-events">
        <h5>History</h5>
        {#each deployEvents as ev}
          <button class="sv-event" onclick={() => sim.requestFrame(ev.frame)}>
            <span class="sv-event-time">{eventTimeLabel(ev)}</span>
            {ev.running}/{ev.total} running
          </button>
        {/each}
      </div>
    {/if}
  </div>
{/if}
