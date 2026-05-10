<script lang="ts">
  import { sim } from "../lib/state.svelte";

  const gen = $derived(sim.gen);

  let customiseOpen = $state(false);

  function markDirty() {
    gen.formDirty = true;
  }

  function blur() {
    gen.maybeSave();
  }

  function toggleRunning() {
    gen.setRunning(!gen.running);
  }
</script>

<div class="gen-section">
  <label class="gen-field">
    <span>Source</span>
    <select
      bind:value={sim.liveSource}
      onchange={() => sim.onLiveSourceChange()}
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
  </label>
</div>

{#if gen.connected}
  <div class="gen-section">
    <div class="gen-status-row">
      <span class="gen-pill" class:running={gen.running}>
        {gen.running ? "Running" : "Paused"}
      </span>
      {#if gen.savedTick}
        <span class="gen-pill saved">Saved ✓</span>
      {/if}
    </div>

    <div class="gen-btns">
      <button class="gen-btn primary" onclick={toggleRunning}>
        {gen.running ? "Pause" : "Resume"}
      </button>
    </div>
  </div>

  <div class="gen-section">
    <div class="gen-grid">
      <label class="gen-field">
        <span title="Average jobs submitted per second">Arrival rate</span>
        <input
          type="number"
          step="0.01"
          bind:value={gen.form.arrival_rate}
          oninput={markDirty}
          onblur={blur}
        />
      </label>
      <label class="gen-field">
        <span
          title="Cap on the sine-wave amplitude for each autoscaled Deployment. 0 turns the autoscaler off."
          >Max replicas / autoscaled deployment</span
        >
        <input
          type="number"
          min="0"
          step="1"
          bind:value={gen.form.deployment_max_replicas}
          oninput={markDirty}
          onblur={blur}
        />
      </label>
    </div>
  </div>

  <div class="gen-section">
    <button
      class="gen-btn customise-toggle"
      onclick={() => (customiseOpen = !customiseOpen)}
    >
      {customiseOpen ? "▾" : "▸"} Customise
    </button>

    {#if customiseOpen}
      <div class="gen-grid">
        <label class="gen-field">
          <span title="Lowest priority assigned to generated jobs"
            >Priority min</span
          >
          <input
            type="number"
            bind:value={gen.form.priority_min}
            oninput={markDirty}
            onblur={blur}
          />
        </label>
        <label class="gen-field">
          <span title="Highest priority assigned to generated jobs"
            >Priority max</span
          >
          <input
            type="number"
            bind:value={gen.form.priority_max}
            oninput={markDirty}
            onblur={blur}
          />
        </label>
        <label class="gen-field">
          <span title="Minimum replicas per generated job">Replica min</span>
          <input
            type="number"
            bind:value={gen.form.replica_min}
            oninput={markDirty}
            onblur={blur}
          />
        </label>
        <label class="gen-field">
          <span title="Maximum replicas per generated job">Replica max</span>
          <input
            type="number"
            bind:value={gen.form.replica_max}
            oninput={markDirty}
            onblur={blur}
          />
        </label>
        <label class="gen-field">
          <span title="Shortest job runtime in seconds">Runtime min</span>
          <input
            type="number"
            step="1"
            bind:value={gen.form.runtime_min}
            oninput={markDirty}
            onblur={blur}
          />
        </label>
        <label class="gen-field">
          <span title="Longest job runtime in seconds">Runtime max</span>
          <input
            type="number"
            step="1"
            bind:value={gen.form.runtime_max}
            oninput={markDirty}
            onblur={blur}
          />
        </label>
        <label class="gen-field">
          <span title="Fraction of jobs generated as gang-scheduled (0–1)"
            >Gang frequency</span
          >
          <input
            type="number"
            step="0.01"
            bind:value={gen.form.gang_frequency}
            oninput={markDirty}
            onblur={blur}
          />
        </label>
        <label class="gen-field">
          <span
            title="Multiplier applied to the Poisson arrival rate at each tick to produce burstier traffic"
            >Burst factor</span
          >
          <input
            type="number"
            step="0.1"
            bind:value={gen.form.burst_factor}
            oninput={markDirty}
            onblur={blur}
          />
        </label>
        <label class="gen-field">
          <span title="Seconds between load-generator ticks (Job submissions)"
            >Loop interval</span
          >
          <input
            type="number"
            step="0.1"
            bind:value={gen.form.loop_interval_seconds}
            oninput={markDirty}
            onblur={blur}
          />
        </label>
        <label class="gen-field">
          <span title="RNG seed for reproducible job generation">Seed</span>
          <input
            type="number"
            bind:value={gen.form.seed}
            oninput={markDirty}
            onblur={blur}
          />
        </label>
        <label class="gen-field full">
          <span title="JSON object mapping quota names to relative weights"
            >Quota weights</span
          >
          <textarea
            bind:value={gen.form.quota_weights}
            oninput={markDirty}
            onblur={blur}
          ></textarea>
        </label>
        <label class="gen-field full">
          <span title="JSON object mapping chip types to relative weights"
            >Chip weights</span
          >
          <textarea
            bind:value={gen.form.chip_weights}
            oninput={markDirty}
            onblur={blur}
          ></textarea>
        </label>
        <label class="gen-field full">
          <span
            title="Nested JSON: chip type → chips per replica → relative weight. A replica must fit on one node."
            >Chips-per-replica weights</span
          >
          <textarea
            bind:value={gen.form.chips_weights}
            oninput={markDirty}
            onblur={blur}
          ></textarea>
        </label>
      </div>
    {/if}
  </div>
{:else}
  <div class="gen-unavailable">
    Generator not available.<br />
    Ensure the loop runner is writing to the shared state directory.
  </div>
{/if}
