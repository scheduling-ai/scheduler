<script lang="ts">
  import { sim } from "../lib/state.svelte";
  import { chipColor } from "../lib/api";

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
  const selCT = $derived(sim.selectedChipType);

  function handleSearchKeydown(e: KeyboardEvent) {
    if (e.key !== "Enter") return;
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
</script>

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
            >&equiv;</span
          > marks deployments (autoscaled)
        </div>
        <div class="help-row">Priority bars: more filled = higher priority</div>
        <div class="help-row">
          Cluster pills in detail cards show replica placement
        </div>
      </div>
    </div>
  </div>
{/if}
