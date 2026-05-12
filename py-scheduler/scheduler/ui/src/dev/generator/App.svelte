<script lang="ts">
  // Generator app: drives the load-generator backend.  This bundle
  // only ships when dev tooling is reachable; customer installs
  // don't deploy the backend service and the UI server 404s this URL.
  import { GeneratorState } from "../../lib/generator.svelte";

  const errorState = $state({ message: "", visible: false });
  let errorTimer: ReturnType<typeof setTimeout> | null = null;

  function showError(msg: string) {
    errorState.message = msg;
    errorState.visible = true;
    if (errorTimer) clearTimeout(errorTimer);
    errorTimer = setTimeout(() => (errorState.visible = false), 5000);
  }

  // Generator state was originally gated on "live mode"; here the app
  // is dedicated so polling is always on.
  const gen = new GeneratorState(
    () => true,
    (msg) => showError(msg),
  );

  $effect(() => {
    gen.startPolling();
  });

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

<header>
  <div class="hdr-row">
    <strong style="margin-right:auto">Fake-job generator</strong>
    {#if gen.connected}
      <span class="gen-pill" class:running={gen.running}>
        {gen.running ? "Running" : "Paused"}
      </span>
    {/if}
    {#if gen.savedTick}
      <span class="gen-pill saved">Saved &check;</span>
    {/if}
    <a class="btn" href="/dev" title="Back to the dev chooser">Chooser</a>
  </div>
</header>

<main id="generator">
  {#if !gen.connected}
    <div class="gen-unavailable">
      Generator not available.<br />
      Either no load-generator backend is deployed (this is normal in customer installs)
      or the UI server can't reach it. Check that
      <code>GENERATOR_URL</code> is set, or that <code>loop-runner</code> is writing
      to the shared state directory in local-dev mode.
    </div>
  {:else}
    <div class="gen-section">
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
  {/if}
</main>

<div class="toast" class:visible={errorState.visible}>{errorState.message}</div>
