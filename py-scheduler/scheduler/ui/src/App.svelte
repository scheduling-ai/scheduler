<script lang="ts">
  import { sim } from "./lib/state.svelte";
  import { escapeHtml, chipColor } from "./lib/api";
  import HomeScreen from "./components/HomeScreen.svelte";
  import Header from "./components/Header.svelte";
  import ClusterGrid from "./components/ClusterGrid.svelte";
  import ScaleView from "./components/ScaleView.svelte";

  let tooltipVisible = $state(false);
  let tooltipX = $state(0);
  let tooltipY = $state(0);
  let tooltipHtml = $state("");
  let tooltipEl: HTMLDivElement;
  let dynamicStyle: HTMLStyleElement | null = null;

  $effect(() => {
    if (!dynamicStyle) {
      dynamicStyle = document.createElement("style");
      dynamicStyle.id = "dynamic-styles";
      document.head.appendChild(dynamicStyle);
    }
    dynamicStyle.textContent = sim.highlightCss;
  });
  $effect(() => {
    document.body.classList.toggle("has-selection", sim.hasSelection);
  });
  $effect(() => {
    const handler = () => sim.handleSliderPointerUp();
    document.addEventListener("pointerup", handler);
    return () => document.removeEventListener("pointerup", handler);
  });
  $effect(() => {
    sim.initFromUrl().catch((e: any) => sim.showError(e.message));
    return () => {
      if (dynamicStyle) {
        dynamicStyle.remove();
        dynamicStyle = null;
      }
    };
  });

  function handleMouseMove(e: MouseEvent) {
    const target = (e.target as HTMLElement).closest(
      ".chip[data-pod], .queue-item[data-pod]",
    ) as HTMLElement | null;
    if (!target || !sim.frames.length) {
      tooltipVisible = false;
      return;
    }
    const podName = target.getAttribute("data-pod")!;
    const phase = target.getAttribute("data-phase") || "queued";
    const frame = sim.displayFrame;
    const pod = frame?.pods?.[podName];
    const type = pod?.chip_type || "Any";

    let html = `<strong>${escapeHtml(podName)}</strong>`;
    html += `<div class="tooltip-grid">`;
    html += `<div class="tooltip-label">Phase</div><div>${escapeHtml(phase)}</div>`;
    const replicas = pod ? (pod.statuses_by_replica || []).length : "?";
    const cpr = pod ? pod.chips_per_replica || 1 : "?";
    html += `<div class="tooltip-label">Request</div><div>${replicas}x${cpr}x <span style="color:${chipColor(String(type))}">${escapeHtml(String(type))}</span></div>`;
    html += `</div>`;

    tooltipHtml = html;
    tooltipVisible = true;
    requestAnimationFrame(() => {
      if (!tooltipEl) return;
      let x = e.clientX + 14;
      let y = e.clientY + 14;
      const rect = tooltipEl.getBoundingClientRect();
      if (x + rect.width > window.innerWidth) x = e.clientX - rect.width - 10;
      if (y + rect.height > window.innerHeight)
        y = e.clientY - rect.height - 10;
      tooltipX = x;
      tooltipY = y;
    });
  }

  function handleClick(e: MouseEvent) {
    const target = (e.target as HTMLElement).closest(
      ".chip[data-pod], .queue-item[data-pod]",
    ) as HTMLElement | null;
    if (target) {
      const podName = target.getAttribute("data-pod")!;
      const gangGroup = target.closest(".gang-group");
      sim.handlePodClick(podName, gangGroup);
    }
  }

  // Chip types in display order for Shift+1..4
  const chipTypeKeys = $derived(
    (sim.parsedView?.chipFree ?? []).map((c) => c.chipType),
  );

  function handleKeydown(e: KeyboardEvent) {
    if (!sim.frames.length) return;

    // Shift+1..4: chip type selection (works even in input)
    if (e.shiftKey && e.key >= "1" && e.key <= "4") {
      const idx = Number(e.key) - 1;
      if (idx < chipTypeKeys.length) {
        e.preventDefault();
        sim.selectChipType(chipTypeKeys[idx]);
        return;
      }
    }

    if (
      (e.target as HTMLElement).tagName === "INPUT" ||
      (e.target as HTMLElement).tagName === "SELECT" ||
      (e.target as HTMLElement).tagName === "TEXTAREA"
    )
      return;
    if (e.code === "Space") {
      e.preventDefault();
      sim.togglePlay();
    } else if (e.code === "ArrowRight") {
      e.preventDefault();
      sim.stepNext();
    } else if (e.code === "ArrowLeft") {
      e.preventDefault();
      sim.stepPrev();
    } else if (e.code === "Escape") {
      e.preventDefault();
      if (sim.selectedCluster) sim.selectCluster(null);
      else if (sim.selectedWorkload) sim.selectWorkload(null);
      else if (sim.selectedQuota) sim.selectQuota(null);
      else if (sim.selectedChipType) sim.selectChipType(null);
    }
  }
</script>

<svelte:window onkeydown={handleKeydown} />

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div onmousemove={handleMouseMove} onclick={handleClick}>
  <HomeScreen />
  <Header />
  {#if sim.selectedCluster}
    <div class="cluster-drill">
      <div class="cluster-drill-bar">
        <button class="cluster-back" onclick={() => sim.selectCluster(null)}
          >&larr; Back</button
        >
        <div class="cluster-pill-row">
          {#each sim.parsedView?.clusters ?? [] as c}
            <button
              class="sv-pill"
              class:active={sim.selectedCluster === c.name}
              onclick={() => sim.selectCluster(c.name)}>{c.name}</button
            >
          {/each}
        </div>
      </div>
      <main id="clusters">
        <ClusterGrid filterCluster={sim.selectedCluster} />
      </main>
    </div>
  {:else}
    <ScaleView />
  {/if}
</div>

<div
  id="tooltip"
  bind:this={tooltipEl}
  style="display:{tooltipVisible
    ? 'block'
    : 'none'}; left:{tooltipX}px; top:{tooltipY}px;"
>
  {@html tooltipHtml}
</div>

<div class="toast" class:visible={sim.errorVisible}>{sim.errorMessage}</div>
