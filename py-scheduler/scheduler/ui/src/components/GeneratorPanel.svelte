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
        <label>Seed</label><input
          type="number"
          bind:value={sim.genForm.seed}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label>Arrival rate</label><input
          type="number"
          step="0.01"
          bind:value={sim.genForm.arrival_rate}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label>Burst factor</label><input
          type="number"
          step="0.1"
          bind:value={sim.genForm.burst_factor}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label>Loop interval</label><input
          type="number"
          step="0.1"
          bind:value={sim.genForm.loop_interval_seconds}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label>Priority min</label><input
          type="number"
          bind:value={sim.genForm.priority_min}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label>Priority max</label><input
          type="number"
          bind:value={sim.genForm.priority_max}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label>Replica min</label><input
          type="number"
          bind:value={sim.genForm.replica_min}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label>Replica max</label><input
          type="number"
          bind:value={sim.genForm.replica_max}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label>Runtime min</label><input
          type="number"
          step="1"
          bind:value={sim.genForm.runtime_min}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label>Runtime max</label><input
          type="number"
          step="1"
          bind:value={sim.genForm.runtime_max}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label>Gang freq</label><input
          type="number"
          step="0.01"
          bind:value={sim.genForm.gang_frequency}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label>Replica fail</label><input
          type="number"
          step="0.01"
          bind:value={sim.genForm.replica_failure_rate}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label>Node fail</label><input
          type="number"
          step="0.01"
          bind:value={sim.genForm.node_failure_rate}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field">
        <label>Node recover</label><input
          type="number"
          step="0.01"
          bind:value={sim.genForm.node_recovery_rate}
          oninput={markDirty}
        />
      </div>
      <div class="gen-field full">
        <label>Quota weights</label><textarea
          bind:value={sim.genForm.quota_weights}
          oninput={markDirty}
        ></textarea>
      </div>
      <div class="gen-field full">
        <label>Chip weights</label><textarea
          bind:value={sim.genForm.chip_weights}
          oninput={markDirty}
        ></textarea>
      </div>
      <div class="gen-field full">
        <label>Chips/replica weights</label><textarea
          bind:value={sim.genForm.chips_weights}
          oninput={markDirty}
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
