<script lang="ts">
  import { getPlayback } from "../lib/context";
  import { chipColor } from "../lib/api";

  let { filterCluster = null }: { filterCluster?: string | null } = $props();

  const sim = getPlayback();

  const visibleClusters = $derived(
    filterCluster
      ? (sim.parsedView?.clusters ?? []).filter((c) => c.name === filterCluster)
      : (sim.parsedView?.clusters ?? []),
  );

  const clusterCaption = $derived.by(() => {
    const frame = sim.displayFrame;
    if (!frame) return "";
    return `Cluster sim at ${sim.tickLabel(sim.currentFrameIdx, frame)} — solver called with this sim as input`;
  });
</script>

{#if clusterCaption}
  <div class="frame-note">{clusterCaption}</div>
{/if}

{#if !sim.parsedView}
  <div class="empty-sim">Load a replay or connect to the live simulator.</div>
{:else if !visibleClusters.length}
  <div class="empty-sim">No cluster data in this frame.</div>
{:else}
  {#each visibleClusters as cluster}
    <section class="cluster">
      <div class="cluster-header">{cluster.name}</div>
      <div class="nodes">
        {#each cluster.nodes as node}
          <div class="node-card">
            <div class="node-head">
              <div class="node-name" title={node.name}>
                {node.name}<span
                  class="node-type"
                  style="color:{chipColor(node.chipType)}">{node.chipType}</span
                >
              </div>
              <div class="node-frac" title="Chips in use / total capacity">
                {node.used}/{node.capacity}
              </div>
            </div>
            <div class="node-chips">
              {#each node.segments as segment}
                <div
                  class="gang-group"
                  class:multi-pod={segment.multiPod}
                  data-gang={segment.gangIdx}
                >
                  {#if segment.chipCount > 1}
                    <span class="gang-num">#{segment.gangIdx}</span>
                  {/if}
                  {#each segment.allocs as alloc}
                    {@const sel = sim.selectedPods.has(alloc.pod)}
                    {#each { length: alloc.chips } as _}
                      <div
                        class="chip phase-{alloc.phase}"
                        class:sv-sel={sel}
                        class:sv-dim={sim.hasSelection && !sel}
                        data-pod={alloc.pod}
                        data-phase={alloc.phase}
                      ></div>
                    {/each}
                  {/each}
                </div>
              {/each}
              {#each { length: node.free } as _}
                <div class="chip free"></div>
              {/each}
            </div>
          </div>
        {/each}
      </div>
    </section>
  {/each}
{/if}
