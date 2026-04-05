<script lang="ts">
  import { sim } from "../lib/state.svelte";

  function markDirty() {
    sim.genFormDirty = true;
  }
</script>

<div class="gen-section">
  <h3>Scheduler View</h3>
  <div class="gen-field">
    <label for="live-scheduler">Solver</label>
    <select
      id="live-scheduler"
      style="width:100%"
      bind:value={sim.liveScheduler}
      onchange={() => sim.onLiveSchedulerChange()}
    >
      {#each sim.solvers as solver}
        <option value={solver.ref}>{solver.name}</option>
      {/each}
    </select>
  </div>
</div>

{#if sim.genConnected}
  <div class="gen-status-row">
    <span class="gen-pill" class:running={sim.genRunning}>
      {sim.genRunning ? "Running" : "Paused"}
    </span>
    <span class="gen-pill" class:running={!sim.genFormDirty}>
      {sim.genFormDirty ? "Unsaved" : "Saved"}
    </span>
  </div>

  <div class="gen-section">
    <h3>Control</h3>
    <div class="gen-btns">
      <button class="gen-btn primary" onclick={() => sim.genStart()}
        >Start</button
      >
      <button class="gen-btn" onclick={() => sim.genPause()}>Pause</button>
      <button class="gen-btn" onclick={() => sim.genResume()}>Resume</button>
      <button class="gen-btn" onclick={() => sim.genSaveConfig()}
        >Save config</button
      >
    </div>
  </div>

  <div class="gen-section">
    <h3>Config</h3>
    <div class="gen-grid">
      <div class="gen-field">
        <label title="RNG seed for reproducible job generation">Seed</label
        ><input
          type="number"
          bind:value={sim.genForm.seed}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label title="Average jobs submitted per second">Arrival rate</label
        ><input
          type="number"
          step="0.01"
          bind:value={sim.genForm.arrival_rate}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label title="Multiplier for periodic traffic spikes"
          >Burst factor</label
        ><input
          type="number"
          step="0.1"
          bind:value={sim.genForm.burst_factor}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label title="Seconds between solver ticks">Loop interval</label><input
          type="number"
          step="0.1"
          bind:value={sim.genForm.loop_interval_seconds}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label title="Lowest priority assigned to generated jobs"
          >Priority min</label
        ><input
          type="number"
          bind:value={sim.genForm.priority_min}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label title="Highest priority assigned to generated jobs"
          >Priority max</label
        ><input
          type="number"
          bind:value={sim.genForm.priority_max}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label title="Minimum replicas per generated job">Replica min</label
        ><input
          type="number"
          bind:value={sim.genForm.replica_min}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label title="Maximum replicas per generated job">Replica max</label
        ><input
          type="number"
          bind:value={sim.genForm.replica_max}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label title="Shortest job runtime in seconds">Runtime min</label><input
          type="number"
          step="1"
          bind:value={sim.genForm.runtime_min}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label title="Longest job runtime in seconds">Runtime max</label><input
          type="number"
          step="1"
          bind:value={sim.genForm.runtime_max}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label title="Fraction of jobs generated as gang-scheduled (0–1)"
          >Gang freq</label
        ><input
          type="number"
          step="0.01"
          bind:value={sim.genForm.gang_frequency}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label title="Probability a replica fails each tick">Replica fail</label
        ><input
          type="number"
          step="0.01"
          bind:value={sim.genForm.replica_failure_rate}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label title="Probability a node fails each tick">Node fail</label
        ><input
          type="number"
          step="0.01"
          bind:value={sim.genForm.node_failure_rate}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label title="Probability a failed node recovers each tick"
          >Node recover</label
        ><input
          type="number"
          step="0.01"
          bind:value={sim.genForm.node_recovery_rate}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field full">
        <label title="JSON object mapping quota names to relative weights"
          >Quota weights</label
        ><textarea bind:value={sim.genForm.quota_weights} oninput={markDirty}
        ></textarea>
      </div>
      <div class="gen-field full">
        <label title="JSON object mapping chip types to relative weights"
          >Chip weights</label
        ><textarea bind:value={sim.genForm.chip_weights} oninput={markDirty}
        ></textarea>
      </div>
      <div class="gen-field full">
        <label
          title="JSON object mapping chips-per-replica counts to relative weights"
          >Chips/replica weights</label
        ><textarea bind:value={sim.genForm.chips_weights} oninput={markDirty}
        ></textarea>
      </div>
    </div>
  </div>
{:else}
  <div class="gen-unavailable">
    Generator not available.<br />
    Ensure the loop runner is writing to the shared state directory.
  </div>
{/if}
