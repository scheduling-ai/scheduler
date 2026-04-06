<script lang="ts">
  import { sim } from "../lib/state.svelte";
  import { chipColor } from "../lib/api";
  import type { Workload, Job, DeploymentGroup } from "../lib/types";

  let searchEl: HTMLInputElement;
  let localSearch = $state("");
  let searchTimer: ReturnType<typeof setTimeout> | null = null;
  let helpOpen = $state(false);
  $effect(() => {
    if (searchEl) searchEl.focus();
  });
  function onSearchInput(value: string) {
    localSearch = value;
    if (searchTimer) clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      sim.globalSearch = value;
    }, 150);
  }

  const selQ = $derived(sim.selectedQuota);
  const selW = $derived(sim.selectedWorkload);
  const selCT = $derived(sim.selectedChipType);

  const quotaDetail = $derived(
    selQ
      ? (sim.parsedView?.quotaSummaries.find((q) => q.name === selQ) ?? null)
      : null,
  );

  // --- Helpers ---

  function depName(id: string): string {
    return sim.parsedView?.deployments.find((d) => d.id === id)?.prefix || id;
  }

  function clusterForNode(nodeName: string): string | null {
    for (const c of sim.parsedView?.clusters ?? []) {
      if (c.nodes.some((n) => n.name === nodeName)) return c.name;
    }
    return null;
  }

  function chipFreeCount(ct: string): number {
    return sim.parsedView?.chipFree.find((c) => c.chipType === ct)?.free ?? 0;
  }

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

  function dotClass(w: Workload): string {
    if (w.kind === "job") {
      const j = w as { kind: "job" } & Job;
      if (j.status === "suspended") return "sv-dot-suspended";
      if (j.status === "pending") return "sv-dot-pending";
      if (j.status === "running" && j.borrowing) return "sv-dot-borrow";
      if (j.status === "running") return "sv-dot-safe";
      if (j.status === "partial") return "sv-dot-partial";
      return "sv-dot-pending";
    }
    const d = w as { kind: "deployment" } & DeploymentGroup;
    if (d.running === 0) return "sv-dot-pending";
    if (d.running < d.total) return "sv-dot-partial";
    if (d.borrowing) return "sv-dot-borrow";
    return "sv-dot-safe";
  }

  function wName(w: Workload): string {
    return w.kind === "job" ? w.name : w.prefix;
  }

  function wId(w: Workload): string {
    return w.kind === "job" ? w.name : w.id;
  }

  // --- Filtering ---

  function matchesFilters(w: Workload): boolean {
    if (selCT && w.chipType !== selCT) return false;
    if (selQ && selQ !== "__all__" && w.quota !== selQ) return false;
    const search = sim.globalSearch.trim().toLowerCase();
    if (search) {
      const name = wName(w).toLowerCase();
      const quota = w.quota.toLowerCase();
      if (!name.includes(search) && !quota.includes(search)) return false;
    }
    return true;
  }

  // --- Column lists ---

  type RenderEntry = {
    w: Workload;
    bracket: "" | "\u250C" | "\u2502" | "\u2514";
  };

  function buildColumn(
    workloads: Workload[],
    isRunning: boolean,
  ): RenderEntry[] {
    const filtered = workloads.filter((w) => {
      if (!matchesFilters(w)) return false;
      if (w.kind === "job") {
        const j = w as { kind: "job" } & Job;
        return isRunning
          ? j.status === "running" || j.status === "partial"
          : j.status === "pending";
      }
      const d = w as { kind: "deployment" } & DeploymentGroup;
      return isRunning ? d.running > 0 : d.running === 0 && d.pending > 0;
    });

    // Sort by priority desc, then name
    filtered.sort(
      (a, b) => b.priority - a.priority || wName(a).localeCompare(wName(b)),
    );

    // Group gang members consecutively
    const ordered: Workload[] = [];
    const placed = new Set<string>();
    for (const w of filtered) {
      const id = wId(w);
      if (placed.has(id)) continue;
      placed.add(id);
      ordered.push(w);
      // If this is a gang job, pull in gang members
      if (w.kind === "job") {
        const j = w as { kind: "job" } & Job;
        if (j.gangMembers.length > 1) {
          for (const m of j.gangMembers) {
            if (m === j.name || placed.has(m)) continue;
            const member = filtered.find(
              (f) => f.kind === "job" && f.name === m,
            );
            if (member) {
              placed.add(m);
              ordered.push(member);
            }
          }
        }
      }
    }

    // Add bracket annotations
    const entries: RenderEntry[] = [];
    for (let i = 0; i < ordered.length; i++) {
      const w = ordered[i];
      let bracket: RenderEntry["bracket"] = "";
      if (w.kind === "job") {
        const j = w as { kind: "job" } & Job;
        if (j.gangMembers.length > 1) {
          const gangGroup = ordered.filter(
            (o) =>
              o.kind === "job" &&
              (o as { kind: "job" } & Job).gangIdx === j.gangIdx,
          );
          const posInGang = gangGroup.indexOf(w);
          if (gangGroup.length > 1) {
            if (posInGang === 0) bracket = "\u250C";
            else if (posInGang === gangGroup.length - 1) bracket = "\u2514";
            else bracket = "\u2502";
          }
        }
      }
      entries.push({ w, bracket });
    }
    return entries;
  }

  const runningCol = $derived.by(() =>
    buildColumn(sim.parsedView?.workloads ?? [], true),
  );
  const pendingCol = $derived.by(() =>
    buildColumn(sim.parsedView?.workloads ?? [], false),
  );

  const runningCount = $derived(runningCol.length);
  const pendingCount = $derived(pendingCol.length);

  // --- Detail card data ---

  const jobDetail = $derived(
    selW ? (sim.parsedView?.jobs.find((j) => j.name === selW) ?? null) : null,
  );
  const deployDetail = $derived(
    selW
      ? (sim.parsedView?.deployments.find((d) => d.id === selW) ?? null)
      : null,
  );

  const jobReplicasByCluster = $derived.by(
    (): { cluster: string; nodes: { node: string; count: number }[] }[] => {
      if (!selW || !sim.displayFrame?.pods) return [];
      const pod = sim.displayFrame.pods[selW];
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
    },
  );

  const queuePos = $derived.by(() => {
    if (!selW) return null;
    const entries = sim.parsedView?.queueEntries ?? [];
    const idx = entries.findIndex((e) => e.name === selW);
    if (idx < 0) return null;
    const ct = jobDetail?.chipType || deployDetail?.chipType;
    const ahead = entries.slice(0, idx).filter((e) => e.chipType === ct);
    return {
      position: ahead.length + 1,
      chipsAhead: ahead.reduce((s, e) => s + e.chips, 0),
    };
  });

  function handleSearchKeydown(e: KeyboardEvent) {
    if (e.key === "Enter") {
      e.preventDefault();
      const q = sim.globalSearch.trim().toLowerCase();
      if (!q) return;
      const quotas = (sim.parsedView?.quotaSummaries ?? [])
        .filter((s) => s.name.toLowerCase().includes(q))
        .map((s) => s.name);
      const workloads = (sim.parsedView?.workloads ?? [])
        .filter((w) =>
          w.kind === "job"
            ? w.name.toLowerCase().includes(q)
            : w.prefix.toLowerCase().includes(q),
        )
        .map((w) => (w.kind === "job" ? w.name : w.id));
      if (quotas.length > 0) {
        sim.selectQuota(quotas[0]);
        sim.globalSearch = "";
        localSearch = "";
      } else if (workloads.length > 0) {
        sim.selectWorkload(workloads[0]);
        sim.globalSearch = "";
        localSearch = "";
      }
    }
  }
</script>

<div class="sv">
  <!-- ═══ FILTER BAR ═══ -->
  <div class="sv-bar">
    <button
      class="sv-help-btn"
      title="Keyboard shortcuts and help"
      onclick={() => (helpOpen = !helpOpen)}>?</button
    >
    <input
      class="sv-search"
      type="text"
      placeholder="Search quotas or workloads..."
      title="Filter workloads and quota pills by name. Press Enter to select first match."
      value={localSearch}
      oninput={(e) => onSearchInput(e.currentTarget.value)}
      bind:this={searchEl}
      onkeydown={handleSearchKeydown}
    />
    {#each sim.parsedView?.chipFree ?? [] as c}
      <button
        class="sv-pill"
        class:active={selCT === c.chipType}
        style="color:{chipColor(c.chipType)}; border-color:{selCT === c.chipType
          ? chipColor(c.chipType)
          : ''}"
        title="Filter by {c.chipType} chip type ({c.free.toLocaleString()} free)"
        onclick={() =>
          sim.selectChipType(selCT === c.chipType ? null : c.chipType)}
      >
        {c.chipType}{#if selCT === c.chipType}
          &times;{/if}
      </button>
    {/each}
    <span class="sv-sep">|</span>
    {#if selQ}
      <button class="sv-pill active" onclick={() => sim.selectQuota(null)}
        >{selQ === "__all__" ? "all-quotas" : selQ} &times;</button
      >
    {:else}
      {@const search = localSearch.trim().toLowerCase()}
      <div class="sv-quota-pills">
        <button
          class="sv-pill sv-pill-sm"
          onclick={() => sim.selectQuota("__all__")}>all-quotas</button
        >
        {#each (sim.parsedView?.quotaSummaries ?? []).filter((qs) => !search || qs.name
              .toLowerCase()
              .includes(search)) as qs}
          <button
            class="sv-pill sv-pill-sm"
            onclick={() => sim.selectQuota(qs.name)}
          >
            {qs.name}
          </button>
        {/each}
      </div>
    {/if}
  </div>

  <!-- ═══ HELP PANEL ═══ -->
  {#if helpOpen}
    <div class="help-panel">
      <div class="help-grid">
        <div class="help-section">
          <h4>Navigation</h4>
          <div class="help-row">
            <kbd>&larr;</kbd> <kbd>&rarr;</kbd> Step through frames
          </div>
          <div class="help-row">
            <kbd>Space</kbd> Play / pause
          </div>
          <div class="help-row">
            <kbd>Esc</kbd> Clear current selection
          </div>
          <div class="help-row">
            <kbd>Shift+1…4</kbd> Select chip type
          </div>
          <div class="help-row">
            Search bar filters by name; <kbd>Enter</kbd> selects first match
          </div>
        </div>
        <div class="help-section">
          <h4>Filters</h4>
          <div class="help-row">
            <strong>Chip pills</strong> — filter by chip type; count shows free chips
          </div>
          <div class="help-row">
            <strong>Quota pills</strong> — filter workloads to one quota
          </div>
          <div class="help-row">
            <strong>all-quotas</strong> — show workloads across every quota
          </div>
        </div>
        <div class="help-section">
          <h4>Status</h4>
          <div class="help-row">
            <span class="help-dot sv-dot-safe"></span> Running within guaranteed quota
          </div>
          <div class="help-row">
            <span class="help-dot sv-dot-borrow"></span> Borrowing — at risk of preemption
          </div>
          <div class="help-row">
            <span class="help-dot sv-dot-partial"></span> Partially scaled
          </div>
          <div class="help-row">
            <span class="help-dot sv-dot-pending"></span> Pending — waiting for resources
          </div>
          <div class="help-row">
            <span class="help-dot sv-dot-suspended"></span> Suspended by scheduler
          </div>
        </div>
        <div class="help-section">
          <h4>Rows</h4>
          <div class="help-row">Click a row to expand its detail card</div>
          <div class="help-row">
            <span class="sv-type-icon" style="display:inline;font-size:14px"
              >≡</span
            > marks deployments (autoscaled)
          </div>
          <div class="help-row">
            Priority bars: more filled = higher priority
          </div>
          <div class="help-row">
            Cluster pills in detail cards show replica placement
          </div>
        </div>
      </div>
    </div>
  {/if}

  <!-- ═══ QUOTA SUMMARY ═══ -->
  {#if quotaDetail}
    <div class="sv-quota-bar">
      <div class="sv-quota-head">
        <span class="sv-quota-title">{quotaDetail.name}</span>
        <button class="sv-x" onclick={() => sim.selectQuota(null)}
          >&times;</button
        >
      </div>
      <div class="sv-card-row">
        {#each quotaDetail.chipBreakdown as cb}
          {@const remaining = cb.guaranteed - cb.used}
          <span class="sv-tag">
            <span style="color:{chipColor(cb.chipType)}">{cb.chipType}</span>
            {#if remaining >= 0}
              <span class="sv-quota-ok">+{remaining} remaining</span>
              <span class="sv-dim">{cb.used} of {cb.guaranteed} used</span>
            {:else}
              <span class="sv-quota-warn">{remaining} over limit</span>
              <span class="sv-dim"
                >{cb.used} used, {cb.guaranteed} guaranteed</span
              >
            {/if}
          </span>
        {/each}
      </div>
      {#if quotaDetail.clusterFree.filter((c) => c.free > 0).length}
        <div class="sv-dim" style="margin-bottom:2px">
          Free chips by cluster:
        </div>
        <div class="sv-cluster-grid">
          {#each quotaDetail.clusterFree
            .filter((c) => c.free > 0)
            .slice(0, 15) as ccf}
            <button
              class="sv-cluster-cell"
              onclick={() => sim.selectCluster(ccf.cluster)}
            >
              <span class="sv-cluster-name">{ccf.cluster}</span>
              <span style="color:{chipColor(ccf.chipType)}"
                >{ccf.free} free</span
              >
            </button>
          {/each}
        </div>
      {/if}
    </div>
  {:else if !selQ}
    <div class="sv-instruction">
      Select a quota above to filter workloads, or choose <button
        class="sv-link-inline"
        onclick={() => sim.selectQuota("__all__")}>all-quotas</button
      > to see everything.
    </div>
  {/if}

  <div class="sv-columns">
    {#each [{ label: "Running", entries: runningCol, count: runningCount }, { label: "Pending", entries: pendingCol, count: pendingCount }] as col}
      <div class="sv-column">
        <div class="sv-col-header">
          {col.label} <span class="sv-col-count">({col.count})</span>
        </div>
        <div class="sv-col-body">
          {#if col.entries.length === 0}
            <div class="sv-empty">
              {col.label === "Pending"
                ? "No pending workloads"
                : "No running workloads"}
            </div>
          {/if}
          {#each col.entries as { w, bracket } (wId(w))}
            {@const sel = selW === wId(w)}
            {@const pri = priDisplay(w.priority)}
            <button
              class="sv-row"
              class:sel
              class:deploy={w.kind === "deployment"}
              onclick={() => sim.selectWorkload(sel ? null : wId(w))}
            >
              <span class="sv-bracket">{bracket}</span>
              <span
                class="sv-type-icon"
                title={w.kind === "deployment" ? "Deployment (autoscaled)" : ""}
                >{w.kind === "deployment" ? "\u2261" : ""}</span
              >
              <span
                class="sv-dot {dotClass(w)}"
                title={dotClass(w) === "sv-dot-safe"
                  ? "Running within guarantee"
                  : dotClass(w) === "sv-dot-borrow"
                    ? "Running on borrowed quota — at risk"
                    : dotClass(w) === "sv-dot-partial"
                      ? "Partially scaled"
                      : dotClass(w) === "sv-dot-pending"
                        ? "Pending"
                        : dotClass(w) === "sv-dot-failed"
                          ? "Failed"
                          : "Suspended"}
              ></span>
              <span class="sv-row-name" title={wName(w)}>{wName(w)}</span>
              <span class="sv-row-chips">
                {#if w.kind === "deployment"}
                  {w.running}/{w.total}
                {:else}
                  {w.totalGpus.toLocaleString()}
                {/if}
                <span
                  class="sv-chip-label"
                  style="color:{chipColor(w.chipType)}"
                  >&times;{w.chipType}</span
                >
              </span>
              <span class="sv-row-quota">{w.quota}</span>
              <span class="sv-row-pri {pri.cls}" title="Priority: {pri.label}">
                {#each Array(4) as _, i}<span
                    class="sv-pri-bar"
                    class:on={i < pri.bars}
                  ></span>{/each}
              </span>
            </button>

            <!-- Inline detail card -->
            {#if sel && w.kind === "job" && jobDetail}
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
                      {@const total = group.nodes.reduce(
                        (s, n) => s + n.count,
                        0,
                      )}
                      <button
                        class="sv-cluster-link"
                        title="{total} replica{total === 1
                          ? ''
                          : 's'} on {group.cluster}"
                        onclick={() => sim.selectCluster(group.cluster)}
                      >
                        {group.cluster} &mdash; {total} replica{total === 1
                          ? ""
                          : "s"}
                      </button>
                    {/each}
                  </div>
                {/if}
                <div class="sv-events">
                  <h5>Events <span class="sv-events-badge">demo</span></h5>
                  <div class="sv-event placeholder">
                    <span class="sv-event-time">t=42</span> Admitted — placed on us-central1-a
                  </div>
                  <div class="sv-event placeholder">
                    <span class="sv-event-time">t=67</span> Preempted — borrowed quota
                    reclaimed
                  </div>
                  <div class="sv-events-wip">
                    Placeholder — real event tracking coming soon
                  </div>
                </div>
              </div>
            {:else if sel && w.kind === "deployment" && deployDetail}
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
                  <span
                    class="sv-tag"
                    style="color:{chipColor(deployDetail.chipType)}"
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
                <div class="sv-events">
                  <h5>Events <span class="sv-events-badge">demo</span></h5>
                  <div class="sv-event placeholder">
                    <span class="sv-event-time">t=12</span> Scaled to 4 replicas —
                    demand spike
                  </div>
                  <div class="sv-event placeholder">
                    <span class="sv-event-time">t=30</span> Replica migrated — node
                    drain on eu-west1-b
                  </div>
                  <div class="sv-events-wip">
                    Placeholder — real event tracking coming soon
                  </div>
                </div>
              </div>
            {/if}
          {/each}
        </div>
      </div>
    {/each}
  </div>
</div>
