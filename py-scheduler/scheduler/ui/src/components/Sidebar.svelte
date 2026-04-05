<script lang="ts">
  import { sim } from "../lib/state.svelte";
  import { chipColor } from "../lib/api";
  import GeneratorPanel from "./GeneratorPanel.svelte";
</script>

<aside>
  <div class="sidebar-tabs">
    <button
      class="sidebar-tab"
      class:active={sim.activeTab === "queue"}
      onclick={() => (sim.activeTab = "queue")}
    >
      Queue <span class="tab-badge">{sim.parsedView?.queue.length ?? 0}</span>
    </button>
    {#if sim.currentMode === "live"}
      <button
        class="sidebar-tab"
        class:active={sim.activeTab === "generator"}
        onclick={() => {
          sim.activeTab = "generator";
          if (!sim.genPolling) sim.startGenPolling();
        }}
      >
        Traffic Gen
      </button>
    {/if}
  </div>

  {#if sim.activeTab === "queue"}
    <div class="tab-pane active">
      <div class="sidebar-head">
        <h2>Queue <span>{sim.queueLabel}</span></h2>
      </div>
      <div class="queue-list">
        {#if !sim.parsedView || !sim.parsedView.queue.length}
          <div class="empty-state">
            {sim.parsedView
              ? "Queue is empty."
              : "Load a replay or connect live."}
          </div>
        {:else}
          {#each sim.parsedView.queue as item}
            <div class="queue-item" data-pod={item.pod}>
              <div class="q-head">
                <div class="q-name" title={item.pod}>{item.pod}</div>
                <div class="q-pri">Pri {item.priority}</div>
              </div>
              <div class="q-info">
                <span class="q-badge">{item.queued}x</span>
                <span class="q-badge"
                  >{item.chips}x<span style="color:{chipColor(item.chipType)}"
                    >{item.chipType}</span
                  ></span
                >
                <span class="q-badge">{item.quota}</span>
              </div>
            </div>
          {/each}
        {/if}
      </div>
    </div>
  {:else}
    <div class="tab-pane active">
      <GeneratorPanel />
    </div>
  {/if}
</aside>
