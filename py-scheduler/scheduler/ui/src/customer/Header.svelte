<script lang="ts">
  // Customer header: playback + meta stats + chip filters + source
  // badge (when multiple bridges are configured).  Deliberately
  // omits the Home button (no chooser in this bundle) and the
  // Traffic-gen toggle (the generator is internal-only — see
  // src/dev/generator/).
  import { getLive } from "../lib/context";
  import { chipColor } from "../lib/api";

  const sim = getLive();

  const chips = $derived(sim.parsedView?.chipFree ?? []);

  const counterText = $derived(
    sim.frames.length
      ? `${sim.currentFrameIdx + 1} / last ${sim.frames.length}`
      : "0 / 0",
  );

  const stats = $derived(sim.summary());
  const statRunningText = $derived(
    sim.displayFrame
      ? `${stats.running.toLocaleString()} replicas running`
      : "--",
  );
  const statQueuedText = $derived(
    sim.displayFrame
      ? `${stats.queued.toLocaleString()} replicas queued`
      : "--",
  );
  const statUtilText = $derived(
    sim.displayFrame ? `${stats.utilization.toFixed(2)}% util` : "--",
  );
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
        title="Scrub through frames"
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

    {#if sim.liveSources.length > 1}
      <div class="hdr-source" title="Source — switch from the deploy config">
        {sim.sourceLabel}
      </div>
    {/if}
  </div>
</header>
