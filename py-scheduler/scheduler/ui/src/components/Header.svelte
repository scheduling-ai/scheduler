<script lang="ts">
  import { sim } from "../lib/state.svelte";
  import { chipColor } from "../lib/api";

  const chips = $derived(sim.parsedView?.chipFree ?? []);

  const counterText = $derived.by(() => {
    if (!sim.frames.length) return "0 / 0";
    if (sim.currentMode === "live")
      return `${sim.currentFrameIdx + 1} / last ${sim.frames.length}`;
    return `${sim.currentFrameIdx + 1} / ${sim.frames.length}`;
  });

  const statRunningText = $derived.by(() => {
    const frame = sim.displayFrame;
    if (!frame) return "--";
    if (sim.currentMode === "live")
      return `${(frame.summary?.running_jobs || 0).toLocaleString()} replicas running`;
    const pv = sim.parsedView;
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
    return `${placed.toLocaleString()} replicas placed`;
  });

  const statQueuedText = $derived.by(() => {
    const frame = sim.displayFrame;
    if (!frame) return "--";
    if (sim.currentMode === "live")
      return `${(frame.summary?.queued_jobs || 0).toLocaleString()} replicas queued`;
    const pv = sim.parsedView;
    if (!pv) return "--";
    return `${pv.queue.reduce((s, q) => s + q.queued, 0).toLocaleString()} replicas queued`;
  });

  const statUtilText = $derived.by(() => {
    const frame = sim.displayFrame;
    if (!frame) return "--";
    if (sim.currentMode === "live")
      return `${(frame.summary?.utilization_percent || 0).toFixed(2)}% util`;
    const pv = sim.parsedView;
    if (!pv) return "--";
    return `${pv.utilization}% util`;
  });
</script>

<header>
  <div class="hdr-row">
    <div class="playback">
      <button
        class="btn-icon"
        disabled={!sim.frames.length ||
          sim.frameBusy ||
          sim.currentFrameIdx === 0}
        aria-label="Previous frame"
        onclick={() => sim.stepPrev()}>&#8592;</button
      >
      <button
        class="btn-icon"
        disabled={!sim.frames.length ||
          sim.frameBusy ||
          sim.currentFrameIdx >= sim.frames.length - 1}
        aria-label="Next frame"
        onclick={() => sim.stepNext()}>&#8594;</button
      >
      <input
        type="range"
        min="0"
        max={Math.max(sim.frames.length - 1, 0)}
        value={sim.sliderValue}
        disabled={!sim.frames.length || (sim.frameBusy && !sim.sliderDragging)}
        title="Scrub through solver frames"
        style="flex:1;"
        onpointerdown={() => sim.handleSliderPointerDown()}
        oninput={(e) =>
          sim.handleSliderInput(Number((e.target as HTMLInputElement).value))}
      />
      <div class="frame-counter">{counterText}</div>
    </div>

    <div class="meta">
      {#each [statRunningText, statQueuedText, statUtilText].filter((t) => t && t !== "--") as text, i}
        {#if i > 0}<span class="meta-sep">&bull;</span>{/if}
        <span>{text}</span>
      {/each}
    </div>

    {#if chips.length}
      <div class="hdr-chips">
        {#each chips as c}
          <button
            class="hdr-chip"
            class:active={sim.selectedChipType === c.chipType}
            style="color:{chipColor(c.chipType)}"
            title="{c.chipType}: {c.free.toLocaleString()} free"
            onclick={() =>
              sim.selectChipType(
                sim.selectedChipType === c.chipType ? null : c.chipType,
              )}
          >
            {c.chipType}: {c.free.toLocaleString()} free
          </button>
        {/each}
      </div>
    {/if}

    {#if sim.currentMode === "live"}
      <button
        class="btn"
        class:active={sim.generatorOpen}
        title="Traffic generator controls"
        onclick={() => {
          sim.generatorOpen = !sim.generatorOpen;
          if (sim.generatorOpen && !sim.gen.polling) sim.gen.startPolling();
        }}
      >
        Traffic gen
        {#if sim.gen.connected && !sim.gen.running}
          <span class="hdr-dot" title="Paused" style="background:#f59e0b"
          ></span>
        {:else if !sim.gen.connected}
          <span class="hdr-dot" title="Disconnected" style="background:#ef4444"
          ></span>
        {/if}
      </button>
    {/if}

    <button
      class="btn"
      title="Return to the home screen"
      onclick={() => sim.openHome()}>Home</button
    >
  </div>
</header>
