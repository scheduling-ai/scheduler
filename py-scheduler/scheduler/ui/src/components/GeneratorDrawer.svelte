<script lang="ts">
  import { sim } from "../lib/state.svelte";
  import GeneratorPanel from "./GeneratorPanel.svelte";

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Escape") {
      e.preventDefault();
      sim.generatorOpen = false;
    }
  }
</script>

<svelte:window onkeydown={handleKeydown} />

{#if sim.generatorOpen}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div
    class="drawer-backdrop"
    onclick={() => (sim.generatorOpen = false)}
  ></div>
  <aside class="drawer" aria-label="Traffic generator">
    <div class="drawer-head">
      <h2>Traffic generator</h2>
      <button
        class="drawer-close"
        aria-label="Close"
        onclick={() => (sim.generatorOpen = false)}>&times;</button
      >
    </div>
    <div class="drawer-body">
      <GeneratorPanel />
    </div>
  </aside>
{/if}
