<script lang="ts">
  // Dev chooser: nothing but a launcher that points at the other
  // bundles.  Each card navigates to a different app; nothing is
  // "in-mode" here, no playback, no state.  Pure landing page.
  import { fetchJson } from "../../lib/api";

  type Scenario = { name: string; description: string };
  type LiveSource = { name: string; label: string; shortLabel?: string };

  let scenarios = $state<Scenario[]>([]);
  let liveSources = $state<LiveSource[]>([]);
  let scenarioName = $state("production_scale");
  let liveSourceName = $state("");

  $effect(() => {
    fetchJson("/scenarios/index.json")
      .then((data) => {
        scenarios = data;
        if (!scenarios.find((s) => s.name === scenarioName))
          scenarioName = scenarios[0]?.name ?? "";
      })
      .catch(() => {
        scenarios = [];
      });
    fetchJson("/api/sources")
      .then((data) => {
        liveSources = Array.isArray(data) ? data : [];
        if (liveSources.length && !liveSourceName)
          liveSourceName = liveSources[0].name;
      })
      .catch(() => {
        liveSources = [];
      });
  });

  const scenarioDescription = $derived(
    scenarios.find((s) => s.name === scenarioName)?.description ?? "",
  );

  function goCustomer() {
    const params = new URLSearchParams();
    if (liveSourceName) params.set("source", liveSourceName);
    window.location.href = params.toString() ? `/?${params.toString()}` : "/";
  }

  function goScenario() {
    if (!scenarioName) return;
    window.location.href = `/dev/replay?scenario=${encodeURIComponent(scenarioName)}`;
  }

  function goReplayBlank() {
    window.location.href = "/dev/replay";
  }

  function goGenerator() {
    window.location.href = "/dev/generator";
  }
</script>

<div id="home">
  <div class="home-panel">
    <section class="home-card">
      <h2>Customer view</h2>
      <p>Open the production live cluster view as a customer would see it.</p>
      {#if liveSources.length > 1}
        <div class="control-group">
          <label for="chooser-source">Source</label>
          <select
            id="chooser-source"
            style="flex:1;"
            bind:value={liveSourceName}
          >
            {#each liveSources as source}
              <option value={source.name}
                >{source.shortLabel || source.label}</option
              >
            {/each}
          </select>
        </div>
      {/if}
      <button class="btn primary" onclick={goCustomer}>Open</button>
    </section>

    <section class="home-card">
      <h2>Replay a scenario</h2>
      <p>
        Load a built-in scenario and step through it frame by frame. The solver
        runs on each tick.
      </p>
      <div class="control-group">
        <label for="chooser-scenario">Scenario</label>
        <select id="chooser-scenario" style="flex:1;" bind:value={scenarioName}>
          {#each scenarios as scenario}
            <option value={scenario.name}>{scenario.name}</option>
          {/each}
        </select>
      </div>
      {#if scenarioDescription}
        <p class="scenario-desc">{scenarioDescription}</p>
      {/if}
      <button class="btn" onclick={goScenario}>Load scenario</button>
    </section>

    <section class="home-card">
      <h2>Replay a JSONL file or URL</h2>
      <p>
        Drop a <code>.jsonl</code> trace from the simulator, live snapshot store,
        or a binder session — drag-and-drop / URL load lives inside the replay app.
      </p>
      <button class="btn" onclick={goReplayBlank}>Open replay</button>
    </section>

    <section class="home-card">
      <h2>Fake-job generator</h2>
      <p>
        Configure the load-generator backend that feeds the local simulator and
        the hosted demo. Not deployed in customer installs.
      </p>
      <button class="btn" onclick={goGenerator}>Open generator</button>
    </section>
  </div>
</div>
