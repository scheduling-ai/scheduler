<script lang="ts">
  import { sim } from "../lib/state.svelte";
  import { chipColor } from "../lib/api";

  const chips = $derived(sim.parsedView?.chipFree ?? []);
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
      <div class="frame-counter">{sim.counterText}</div>
    </div>

    <div class="meta">
      {#each [sim.statRunningText, sim.statQueuedText, sim.statUtilText].filter((t) => t && t !== "--") as text, i}
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

    <button
      class="btn"
      title="Return to the home screen"
      onclick={() => sim.openHome()}>Home</button
    >
  </div>
</header>
