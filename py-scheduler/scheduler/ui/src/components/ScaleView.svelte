<script lang="ts">
  import { sim } from "../lib/state.svelte";
  import { chipColor } from "../lib/api";
  import type { JobHistoryEvent } from "../lib/types";

  let searchEl: HTMLInputElement;
  $effect(() => {
    if (searchEl) searchEl.focus();
  });

  const selQ = $derived(sim.selectedQuota);
  const selW = $derived(sim.selectedWorkload);
  const selCT = $derived(sim.selectedChipType);

  const quotaDetail = $derived(
    selQ
      ? (sim.parsedView?.quotaSummaries.find((q) => q.name === selQ) ?? null)
      : null,
  );
  const jobDetail = $derived(
    selW ? (sim.parsedView?.jobs.find((j) => j.name === selW) ?? null) : null,
  );
  const deployDetail = $derived(
    selW
      ? (sim.parsedView?.deployments.find((d) => d.id === selW) ?? null)
      : null,
  );

  // Which chip types to show
  const visibleChips = $derived.by((): Set<string> | null => {
    if (selCT) return new Set([selCT]);
    if (jobDetail) return new Set([jobDetail.chipType]);
    if (deployDetail) return new Set([deployDetail.chipType]);
    if (quotaDetail) {
      const s = new Set(quotaDetail.chipBreakdown.map((c) => c.chipType));
      return s.size > 0 ? s : null;
    }
    return null;
  });

  // Queue grouped by chip
  const queueByChip = $derived.by(() => {
    const entries = sim.parsedView?.queueEntries ?? [];
    const search = sim.globalSearch.trim().toLowerCase();
    const groups = new Map<string, (typeof entries)[number][]>();
    for (const e of entries) {
      if (visibleChips && !visibleChips.has(e.chipType)) continue;
      if (search) {
        const name = e.kind === "job" ? e.name : depName(e.name) || e.quota;
        if (
          !name.toLowerCase().includes(search) &&
          !e.quota.toLowerCase().includes(search)
        )
          continue;
      }
      const arr = groups.get(e.chipType) || [];
      arr.push(e);
      groups.set(e.chipType, arr);
    }
    return Array.from(groups.entries()).sort((a, b) =>
      a[0].localeCompare(b[0]),
    );
  });

  // Search results
  const searchResults = $derived.by(() => {
    const q = sim.globalSearch.trim().toLowerCase();
    if (!q) return { quotas: [] as string[], workloads: [] as string[] };
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
    return { quotas, workloads };
  });

  // Job history + replicas
  const jobHistory = $derived.by((): JobHistoryEvent[] => {
    if (!selW || !jobDetail) return [];
    return sim.jobHistory(selW);
  });
  const jobReplicas = $derived.by(() => {
    if (!selW || !sim.displayFrame?.pods) return [];
    const pod = sim.displayFrame.pods[selW];
    if (!pod) return [];
    const map = new Map<string, { phase: string; count: number }>();
    for (const r of pod.statuses_by_replica || []) {
      const key = `${r.node ?? "unplaced"}|${r.phase}`;
      const entry = map.get(key);
      if (entry) entry.count++;
      else map.set(key, { phase: r.phase, count: 1 });
    }
    return Array.from(map.entries())
      .map(([k, v]) => ({ node: k.split("|")[0], ...v }))
      .sort((a, b) => b.count - a.count);
  });
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

  function depName(id: string): string {
    return sim.parsedView?.deployments.find((d) => d.id === id)?.prefix || id;
  }

  function handleSearchKeydown(e: KeyboardEvent) {
    if (e.key === "Enter") {
      e.preventDefault();
      if (searchResults.quotas.length > 0) {
        sim.selectQuota(searchResults.quotas[0]);
        sim.globalSearch = "";
      } else if (searchResults.workloads.length > 0) {
        sim.selectWorkload(searchResults.workloads[0]);
        sim.globalSearch = "";
      }
    }
  }

  function chipFree(ct: string): number {
    return sim.parsedView?.chipFree.find((c) => c.chipType === ct)?.free ?? 0;
  }
</script>

<div class="sv">
  <!-- Filter bar -->
  <div class="sv-bar">
    <input
      class="sv-search"
      type="text"
      placeholder="Search quotas or workloads..."
      bind:value={sim.globalSearch}
      bind:this={searchEl}
      onkeydown={handleSearchKeydown}
    />
    {#if selQ}
      <button class="sv-pill" onclick={() => sim.selectQuota(null)}
        >{selQ} &times;</button
      >
    {/if}
    {#if selCT}
      <button
        class="sv-pill"
        style="border-color:{chipColor(selCT)}; color:{chipColor(selCT)}"
        onclick={() => sim.selectChipType(null)}>{selCT} &times;</button
      >
    {/if}
    {#if selW}
      <button class="sv-pill" onclick={() => sim.selectWorkload(null)}
        >{jobDetail?.name || depName(selW)} &times;</button
      >
    {/if}
  </div>

  <div class="sv-main">
    <!-- ═══ DETAIL CARD (above queue when something is selected) ═══ -->

    {#if jobDetail}
      <div class="sv-card">
        <div class="sv-card-head">
          {jobDetail.name}
          <button class="sv-x" onclick={() => sim.selectWorkload(null)}
            >&times;</button
          >
        </div>
        <div class="sv-card-row">
          <span class="sv-tag">Pri {jobDetail.priority}</span>
          <span class="sv-tag"
            >{jobDetail.totalGpus}<span
              style="color:{chipColor(jobDetail.chipType)}"
              >&times;{jobDetail.chipType}</span
            ></span
          >
          <span class="sv-tag"
            >{jobDetail.replicas}r&times;{jobDetail.chipsPerReplica}c</span
          >
          <span class="sv-tag">{jobDetail.quota}</span>
          <span class="sv-tag">{jobDetail.status}</span>
        </div>
        {#if jobHistory.length > 1}
          <div class="sv-tl">
            <div class="sv-tl-bar">
              {#each jobHistory as event, i}
                {@const next = jobHistory[i + 1]}
                {@const end = next
                  ? next.frame
                  : Math.max(sim.frames.length, 1)}
                {@const w =
                  ((end - event.frame) / Math.max(sim.frames.length, 1)) * 100}
                <div class="sv-tl-{event.status}" style="width:{w}%"></div>
              {/each}
            </div>
            <div class="sv-tl-links">
              {#each jobHistory as event}
                {#if event.status !== "absent"}
                  <button
                    class="sv-link"
                    onclick={() => sim.requestFrame(event.frame)}
                    >t={event.frame} {event.status}</button
                  >
                {/if}
              {/each}
            </div>
          </div>
        {/if}
        {#if queuePos}
          <div class="sv-card-note">
            Pos {queuePos.position} &middot; {queuePos.chipsAhead.toLocaleString()}
            {jobDetail.chipType} ahead &middot;
            {chipFree(jobDetail.chipType).toLocaleString()} free (need {jobDetail.totalGpus})
          </div>
        {/if}
        {#if jobDetail.status === "running" && quotaDetail}
          {@const cb = quotaDetail.chipBreakdown.find(
            (c) => c.chipType === jobDetail.chipType,
          )}
          {#if cb && cb.guaranteed > 0 && cb.used > cb.guaranteed}
            <div class="sv-card-warn">
              Borrowing {cb.used - cb.guaranteed}
              {jobDetail.chipType} over guarantee
            </div>
          {/if}
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
        {#if jobReplicas.length}
          <div class="sv-placement">
            {#each jobReplicas as r}
              <span class="sv-node">{r.node} &times;{r.count} {r.phase}</span>
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
          <span class="sv-tag">Pri {deployDetail.priority}</span>
          <span class="sv-tag">{deployDetail.total.toLocaleString()} pods</span>
          <span class="sv-tag" style="color:{chipColor(deployDetail.chipType)}"
            >{deployDetail.chipType}</span
          >
          <span class="sv-tag">{deployDetail.quota}</span>
        </div>
        <div class="sv-card-note">
          {deployDetail.running.toLocaleString()} running &middot;
          {deployDetail.pending.toLocaleString()} pending
        </div>
        {#if Object.keys(deployDetail.clusterCounts).length}
          <div class="sv-placement">
            {#each Object.entries(deployDetail.clusterCounts).sort((a, b) => b[1] - a[1]) as [cl, ct]}
              <span class="sv-node">{cl} &times;{ct}</span>
            {/each}
          </div>
        {/if}
      </div>
    {:else if quotaDetail}
      <div class="sv-card">
        <div class="sv-card-head">
          {quotaDetail.name}
          <button class="sv-x" onclick={() => sim.selectQuota(null)}
            >&times;</button
          >
        </div>
        <div class="sv-card-row">
          {#each quotaDetail.chipBreakdown as cb}
            {@const over =
              cb.guaranteed > 0 && cb.used > cb.guaranteed
                ? cb.used - cb.guaranteed
                : 0}
            <span class="sv-tag">
              <span style="color:{chipColor(cb.chipType)}">{cb.chipType}</span>
              {cb.used}/{cb.guaranteed}g
              {#if over > 0}<span class="sv-over">+{over}</span>{/if}
            </span>
          {/each}
        </div>
        {#if quotaDetail.clusterFree.filter((c) => c.free > 0).length}
          <div class="sv-card-row">
            <span class="sv-dim">Free:</span>
            {#each quotaDetail.clusterFree
              .filter((c) => c.free > 0)
              .slice(0, 8) as ccf}
              <span class="sv-tag"
                >{ccf.cluster}
                <span style="color:{chipColor(ccf.chipType)}">{ccf.free}</span
                ></span
              >
            {/each}
          </div>
        {/if}
        {#if quotaDetail.workloads.length}
          <div class="sv-card-workloads">
            {#each quotaDetail.workloads as w}
              <button
                class="sv-wl"
                onclick={() =>
                  sim.selectWorkload(w.kind === "job" ? w.name : w.id)}
              >
                {w.kind === "job" ? w.name : w.prefix}
                <span class="sv-dim">
                  {#if w.kind === "job"}
                    {w.totalGpus}<span style="color:{chipColor(w.chipType)}"
                      >&times;{w.chipType}</span
                    >
                    {w.status}
                  {:else}
                    {w.total.toLocaleString()} pods
                  {/if}
                </span>
              </button>
            {/each}
          </div>
        {/if}
      </div>
    {:else if selCT}
      {@const info = sim.parsedView?.chipFree.find((c) => c.chipType === selCT)}
      <div class="sv-card">
        <div class="sv-card-head" style="color:{chipColor(selCT)}">
          {selCT}
          <button class="sv-x" onclick={() => sim.selectChipType(null)}
            >&times;</button
          >
        </div>
        {#if info}
          <div class="sv-card-note">
            {info.free.toLocaleString()} free / {info.total.toLocaleString()} total
          </div>
        {/if}
        <div class="sv-placement">
          {#each (sim.parsedView?.clusterChipFree ?? []).filter((c) => c.chipType === selCT && c.free > 0) as ccf}
            <span class="sv-node">{ccf.cluster} {ccf.free}/{ccf.total}</span>
          {/each}
        </div>
      </div>
    {/if}

    <!-- ═══ QUEUE (full width, grouped by chip type) ═══ -->

    {#if sim.globalSearch.trim() && !selQ && !selW && !selCT}
      <!-- Search results mode -->
      {#if searchResults.quotas.length}
        <div class="sv-chip-card">
          <div class="sv-chip-head">Quotas</div>
          {#each searchResults.quotas as qName}
            <button
              class="sv-qrow"
              onclick={() => {
                sim.selectQuota(qName);
                sim.globalSearch = "";
              }}
            >
              <span class="sv-qrow-name">{qName}</span>
            </button>
          {/each}
        </div>
      {/if}
      {#if searchResults.workloads.length}
        <div class="sv-chip-card">
          <div class="sv-chip-head">Workloads</div>
          {#each searchResults.workloads as wId}
            {@const job = sim.parsedView?.jobs.find((j) => j.name === wId)}
            {@const dep = sim.parsedView?.deployments.find((d) => d.id === wId)}
            <button
              class="sv-qrow"
              onclick={() => {
                sim.selectWorkload(wId);
                sim.globalSearch = "";
              }}
            >
              <span class="sv-qrow-name">{job?.name || dep?.prefix || wId}</span
              >
              <span class="sv-qrow-chips">
                {#if job}{job.totalGpus}
                  <span style="color:{chipColor(job.chipType)}"
                    >{job.chipType}</span
                  >
                  {job.status}
                {:else if dep}{dep.total.toLocaleString()} pods
                  <span style="color:{chipColor(dep.chipType)}"
                    >{dep.chipType}</span
                  >{/if}
              </span>
            </button>
          {/each}
        </div>
      {/if}
      {#if !searchResults.quotas.length && !searchResults.workloads.length}
        <div class="sv-empty">No matches</div>
      {/if}
    {:else}
      {#each queueByChip as [chipType, entries]}
        {@const free = chipFree(chipType)}
        <div class="sv-chip-card">
          <div class="sv-chip-head">
            <span style="color:{chipColor(chipType)}">{chipType}</span>
            <span class="sv-chip-free">{free.toLocaleString()} free</span>
          </div>
          {#each entries as entry, i}
            {@const isMyQ = selQ && entry.quota === selQ}
            {@const hidden = selQ != null && !selW && !isMyQ}
            {@const dim = selQ != null && selW != null && !isMyQ}
            {@const sel = selW === entry.name}
            {@const cumul = entries
              .slice(0, i + 1)
              .reduce((s, e) => s + e.chips, 0)}
            {#if !hidden}
              <button
                class="sv-qrow"
                class:sel
                class:dim
                onclick={() => sim.selectWorkload(entry.name)}
              >
                <span class="sv-qrow-pri">{entry.priority}</span>
                <span class="sv-qrow-name" class:rollup={entry.kind !== "job"}>
                  {entry.kind === "job" ? entry.name : depName(entry.name)}
                </span>
                <span class="sv-qrow-chips">{entry.chips.toLocaleString()}</span
                >
                <span class="sv-qrow-cumul" class:over={cumul > free}
                  >{cumul.toLocaleString()}</span
                >
                <span class="sv-qrow-quota">{entry.quota}</span>
              </button>
            {/if}
          {/each}
        </div>
      {/each}
    {/if}
  </div>
</div>
