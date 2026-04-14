<script lang="ts">
  import { sim } from "../lib/state.svelte";
  import { chipColor } from "../lib/api";
  import type { Workload, Job, DeploymentGroup } from "../lib/types";
  import FilterBar from "./FilterBar.svelte";
  import WorkloadDetail from "./WorkloadDetail.svelte";

  const selQ = $derived(sim.selectedQuota);
  const selW = $derived(sim.selectedWorkload);
  const selCT = $derived(sim.selectedChipType);

  const quotaDetail = $derived(
    selQ
      ? (sim.parsedView?.quotaSummaries.find((q) => q.name === selQ) ?? null)
      : null,
  );

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
</script>

<div class="sv">
  <FilterBar />

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
    {#each [{ label: "Running", entries: runningCol }, { label: "Pending", entries: pendingCol }] as col}
      <div class="sv-column">
        <div class="sv-col-header">
          {col.label} <span class="sv-col-count">({col.entries.length})</span>
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

            {#if sel}
              <WorkloadDetail workload={w} />
            {/if}
          {/each}
        </div>
      </div>
    {/each}
  </div>
</div>
