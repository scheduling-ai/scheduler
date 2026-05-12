<script lang="ts">
  // Replay header: playback + meta + chip filters + replay-specific
  // controls (re-solve, solver picker, run-solver toggle, back to
  // chooser).
  import { getReplay } from "../../lib/context";
  import { chipColor } from "../../lib/api";

  const sim = getReplay();

  const chips = $derived(sim.parsedView?.chipFree ?? []);

  const counterText = $derived(
    sim.frames.length
      ? `${sim.currentFrameIdx + 1} / ${sim.frames.length}`
      : "0 / 0",
  );

  const stats = $derived(sim.summary());
  const statRunningText = $derived(
    sim.displayFrame
      ? `${stats.running.toLocaleString()} replicas placed`
      : "--",
  );
  const statQueuedText = $derived(
    sim.displayFrame
      ? `${stats.queued.toLocaleString()} replicas queued`
      : "--",
  );
  const statUtilText = $derived(
    sim.displayFrame ? `${stats.utilization}% util` : "--",
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
        title="Scrub through solver frames"
        style="flex:1;"
        onpointerdown={() => sim.handleSliderPointerDown()}
        oninput={(e) =>
          sim.handleSliderInput(Number((e.target as HTMLInputElement).value))}
      />
      <div class="frame-counter">{counterText}</div>
      {#if sim.replayRunSolver}
        <button
          class="btn-icon"
          disabled={!sim.frames.length || sim.frameBusy}
          aria-label="Re-solve current frame"
          title="Re-solve the current frame (drops the cached result and re-runs /api/solve — useful when debugging the solver)."
          onclick={() => sim.resolveCurrentFrame()}>&#x21bb;</button
        >
      {/if}
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

    <label class="hdr-toggle" title="Run the solver between frames">
      <input
        type="checkbox"
        checked={sim.replayRunSolver}
        onchange={(e) =>
          sim.onReplayRunSolverChange((e.target as HTMLInputElement).checked)}
      />
      solver
    </label>

    {#if sim.solvers.length > 1}
      <select
        class="hdr-select"
        value={sim.replaySolver}
        onchange={(e) => {
          sim.replaySolver = (e.target as HTMLSelectElement).value;
          sim.onReplaySolverChange();
        }}
      >
        {#each sim.solvers as solver}
          <option value={solver.ref}>{solver.name}</option>
        {/each}
      </select>
    {/if}

    <a class="btn" href="/dev" title="Back to the dev chooser">Chooser</a>
  </div>
</header>
