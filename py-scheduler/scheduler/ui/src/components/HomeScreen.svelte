<script lang="ts">
  import { sim } from "../lib/state.svelte";

  let fileInput: HTMLInputElement;
  let dragover = $state(false);

  const scenarioDescription = $derived(
    sim.scenarios.find((s) => s.name === sim.currentScenarioName)
      ?.description ?? "",
  );

  // Verbose label for the currently selected live source.  Surfaced as
  // helper text under the dropdown because the dropdown itself shows
  // the compact `shortLabel` (the verbose `label` would truncate).
  const liveSourceDescription = $derived(
    sim.liveSources.find((s) => s.name === sim.homeLiveSource)?.label ?? "",
  );

  function onDrop(e: DragEvent) {
    e.preventDefault();
    dragover = false;
    if (e.dataTransfer?.files.length) sim.handleFile(e.dataTransfer.files[0]);
  }

  function onFileChange(e: Event) {
    const input = e.target as HTMLInputElement;
    if (input.files?.length) sim.handleFile(input.files[0]);
    input.value = "";
  }
</script>

{#if sim.homeVisible}
  <div id="home">
    <div class="home-panel">
      <section class="home-card">
        <h2>Live stream</h2>
        <p>
          Watch a live cluster: see what's running, what's queued, and where
          each replica landed.
        </p>
        <div class="control-group">
          <label for="home-live-source">Source</label>
          <select
            id="home-live-source"
            style="flex:1;"
            bind:value={sim.homeLiveSource}
          >
            {#if sim.liveSources.length}
              {#each sim.liveSources as source}
                <option value={source.name}
                  >{source.shortLabel || source.label}</option
                >
              {/each}
            {:else}
              {#each sim.solvers as solver}
                <option value={solver.ref}>{solver.name}</option>
              {/each}
            {/if}
          </select>
        </div>
        {#if liveSourceDescription}
          <p class="scenario-desc">{liveSourceDescription}</p>
        {/if}
        <button
          class="btn primary"
          onclick={() => {
            sim.liveSource = sim.homeLiveSource;
            sim.bootstrapLive().catch((e) => sim.showError(e.message));
          }}>Connect live</button
        >
      </section>

      <section class="home-card">
        <h2>Built-in scenario</h2>
        <p>
          Load a finite scenario and step through it frame by frame. The solver
          runs on each timestep to produce the next state.
        </p>
        <div class="control-group">
          <label for="scenario-select">Scenario</label>
          <select
            id="scenario-select"
            style="flex:1;"
            bind:value={sim.currentScenarioName}
          >
            {#each sim.scenarios as scenario}
              <option
                value={scenario.name}
                selected={scenario.name === "production_scale"}
              >
                {scenario.name}
              </option>
            {/each}
          </select>
        </div>
        {#if scenarioDescription}
          <p class="scenario-desc">{scenarioDescription}</p>
        {/if}
        <div class="control-group">
          <label for="home-scenario-solver">Solver</label>
          <select
            id="home-scenario-solver"
            style="flex:1;"
            bind:value={sim.homeScenarioSolver}
          >
            {#each sim.solvers as solver}
              <option value={solver.ref}>{solver.name}</option>
            {/each}
          </select>
        </div>
        <button
          class="btn"
          onclick={() => {
            sim
              .loadScenario({ name: sim.currentScenarioName || undefined })
              .catch((e) => sim.showError(e.message));
          }}>Load scenario</button
        >
      </section>

      <section class="home-card">
        <h2>Replay file</h2>
        <p>
          Drop a <code>.jsonl</code> trace from the existing simulator, live snapshot
          store, or recorded binder session.
        </p>
        <div
          class="drop-target"
          class:dragover
          ondragenter={(e) => {
            e.preventDefault();
            dragover = true;
          }}
          ondragover={(e) => {
            e.preventDefault();
            dragover = true;
          }}
          ondragleave={(e) => {
            e.preventDefault();
            dragover = false;
          }}
          ondrop={onDrop}
        >
          <div>
            <div style="font-weight:600; margin-bottom:8px;">
              Drop file here
            </div>
            <div
              style="font-size:13px; color:var(--text-dim); margin-bottom:16px;"
            >
              or
            </div>
            <button class="btn" onclick={() => fileInput.click()}
              >Choose file</button
            >
          </div>
        </div>
        <input
          type="file"
          accept=".jsonl,.json,.txt"
          hidden
          bind:this={fileInput}
          onchange={onFileChange}
        />
      </section>
    </div>
  </div>
{/if}
