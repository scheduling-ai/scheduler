// Shared playback state: frames buffer, slider/keyboard navigation,
// selection, error toast.  Subclasses (LiveState, ReplayState) own how
// frames get loaded and what each tick means — everything else is the
// same across modes.
//
// The abstract methods are the seam: `loadAndDisplay` orchestrates the
// per-mode pipeline for moving to frame N (live just reads the buffer;
// replay solves up to N first), `tickLabel` lets the cluster grid
// render "seq 14" vs "t=14" without checking the subclass, and so on.

import { parseFrame } from "./parse";
import type { Frame } from "./types";

export type SummaryStats = {
  running: number;
  queued: number;
  utilization: number;
};

export abstract class PlaybackCore {
  // Frames buffer + currently displayed frame.
  frames = $state<Frame[]>([]);
  currentFrameIdx = $state(0);
  displayFrame = $state<Frame | null>(null);

  // Playback timer.
  playing = $state(false);
  fps = $state(5);
  playAnimId: number | null = null;

  // Slider.  sliderValue is what the user sees during a drag;
  // sliderRequestedIdx is what we'll request when they release.
  sliderValue = $state(0);
  sliderDragging = $state(false);
  sliderRequestedIdx = $state(0);
  sliderInputTimer: ReturnType<typeof setTimeout> | null = null;

  // Frame request coordination.  Every requestFrame call bumps
  // frameRequestId; any in-flight async work checks the id before
  // mutating state so we don't race when the user scrubs faster than
  // the solver returns.
  frameBusy = $state(false);
  frameRequestId = 0;
  queuedFrameIndex: number | null = null;

  // Selection state.  selectedPods/selectedGangIdx/selectedPod are
  // related — clicking a pod in a gang of size > 1 sets gangIdx; a
  // singleton click sets selectedPod.
  selectedPod = $state<string | null>(null);
  selectedGangIdx = $state<number | null>(null);
  selectedQuota = $state<string | null>(null);
  selectedWorkload = $state<string | null>(null);
  selectedChipType = $state<string | null>(null);
  selectedCluster = $state<string | null>(null);
  globalSearch = $state("");

  // Overlays.
  helpOpen = $state(false);

  // Toast.  showError(msg) flashes the toast for ~5s.
  errorMessage = $state("");
  errorVisible = $state(false);
  errorTimer: ReturnType<typeof setTimeout> | null = null;

  // Derived view of the current frame.  Components read this rather
  // than parsing the raw frame themselves.
  parsedView = $derived.by(() => parseFrame(this.displayFrame));

  selectedPods = $derived.by<Set<string>>(() => {
    if (this.selectedGangIdx != null) {
      const set = this.parsedView?.gangInfo.gangSetMembers.get(
        this.selectedGangIdx,
      );
      return set ? new Set(set) : new Set();
    }
    if (this.selectedPod) return new Set([this.selectedPod]);
    return new Set();
  });
  hasSelection = $derived(this.selectedPods.size > 0);

  // ── Subclass seam ──

  /** Display whichever frame should be shown at `idx`.  Live just
   *  reads the buffer; replay solves up to `idx` first. */
  abstract loadAndDisplay(idx: number, requestId: number): Promise<void>;

  /** Returns the frame to use for *historical* lookups (job /
   *  deployment history walks past frames). Returns null if the
   *  needed frame can't be reconstructed yet (e.g. an upstream solve
   *  hasn't run). */
  abstract getHistoricalFrame(idx: number): Frame | null;

  /** Label for the "frame N of M" badge.  Live = "seq 14" (matches
   *  what k8s exposes), replay = "t=14" (no real-time clock). */
  abstract tickLabel(idx: number, frame: Frame | null): string;

  /** Summary stats for the header — live reads frame.summary which
   *  the bridge fills in; replay computes from the displayed frame. */
  abstract summary(): SummaryStats;

  /** Update the browser URL to match current state.  Each app owns
   *  its own URL shape; the base only debounces. */
  protected abstract writeRoute(): void;

  // ── Selection ──

  selectQuota(name: string | null) {
    this.selectedQuota = this.selectedQuota === name ? null : name;
    this.selectedWorkload = null;
  }

  selectWorkload(name: string | null) {
    this.selectedWorkload = this.selectedWorkload === name ? null : name;
    if (this.selectedWorkload && this.parsedView) {
      const job = this.parsedView.jobs.find(
        (j) => j.name === this.selectedWorkload,
      );
      if (job) {
        this.selectedQuota = job.quota;
      } else {
        const dep = this.parsedView.deployments.find(
          (d) => d.id === this.selectedWorkload,
        );
        if (dep) this.selectedQuota = dep.quota;
      }
    }
  }

  selectChipType(ct: string | null) {
    this.selectedChipType = this.selectedChipType === ct ? null : ct;
    this.selectedWorkload = null;
  }

  selectCluster(name: string | null) {
    this.selectedCluster = name;
  }

  clearSelection() {
    this.selectedPod = null;
    this.selectedGangIdx = null;
    this.selectedQuota = null;
    this.selectedWorkload = null;
    this.selectedChipType = null;
    this.selectedCluster = null;
  }

  clearSmallScaleSelection() {
    this.selectedPod = null;
    this.selectedGangIdx = null;
  }

  handlePodClick(podName: string, gangGroupElement: Element | null) {
    const gangIdx = gangGroupElement
      ? Number(gangGroupElement.getAttribute("data-gang"))
      : (this.parsedView?.gangInfo.podToIdx.get(podName) ?? null);
    const gangMembers =
      gangIdx != null
        ? this.parsedView?.gangInfo.gangSetMembers.get(gangIdx)
        : null;
    if (gangMembers && gangMembers.size > 1) {
      this.selectedGangIdx = this.selectedGangIdx === gangIdx ? null : gangIdx;
      this.selectedPod = null;
    } else {
      this.selectedPod = this.selectedPod === podName ? null : podName;
      this.selectedGangIdx = null;
    }
  }

  // ── History walks ──
  //
  // jobHistory / deploymentHistory iterate getHistoricalFrame so they
  // work identically across live and replay; the per-mode bit is
  // "how do I reconstruct the frame at index i?", which lives in the
  // subclasses.

  jobHistory(jobName: string): {
    frame: number;
    seq: number | null;
    timestamp: string | null;
    status: string;
  }[] {
    const events: {
      frame: number;
      seq: number | null;
      timestamp: string | null;
      status: string;
    }[] = [];
    let lastStatus = "";
    let everSeen = false;
    for (let i = 0; i < this.frames.length; i++) {
      const f = this.getHistoricalFrame(i);
      if (!f) break;
      const pod = f?.pods?.[jobName];
      let status = "absent";
      if (pod) {
        const placed = (pod.statuses_by_replica || []).some((r) => r.node);
        const allSuspended =
          (pod.statuses_by_replica || []).length > 0 &&
          (pod.statuses_by_replica || []).every((r) => r.phase === "suspended");
        if (allSuspended) status = "suspended";
        else if (placed) status = "running";
        else status = "pending";
      }
      if (status !== "absent") everSeen = true;
      if (!everSeen) continue;
      const display = status === "absent" ? "ended" : status;
      if (display !== lastStatus) {
        events.push({
          frame: i,
          seq: f?.seq ?? null,
          timestamp: f?.timestamp ?? null,
          status: display,
        });
        lastStatus = display;
      }
    }
    return events;
  }

  deploymentHistory(
    prefix: string,
    quota: string,
    chipType: string,
    priority: number,
    chipsPerReplica: number,
  ): {
    frame: number;
    seq: number | null;
    timestamp: string | null;
    running: number;
    total: number;
  }[] {
    const events: {
      frame: number;
      seq: number | null;
      timestamp: string | null;
      running: number;
      total: number;
    }[] = [];
    let lastKey = "";
    let everSeen = false;
    for (let i = 0; i < this.frames.length; i++) {
      const f = this.getHistoricalFrame(i);
      if (!f) break;
      let running = 0;
      let total = 0;
      for (const [podName, pod] of Object.entries(f?.pods ?? {})) {
        const p = podName.includes("-")
          ? podName.substring(0, podName.lastIndexOf("-"))
          : podName;
        if (
          p !== prefix ||
          (pod.quota || "default") !== quota ||
          (pod.chip_type || "") !== chipType ||
          (pod.priority || 0) !== priority ||
          (pod.chips_per_replica || 1) !== chipsPerReplica
        )
          continue;
        for (const r of pod.statuses_by_replica || []) {
          total++;
          if (r.node) running++;
        }
      }
      if (total > 0) everSeen = true;
      if (!everSeen) continue;
      const key = `${running}/${total}`;
      if (key !== lastKey) {
        events.push({
          frame: i,
          seq: f?.seq ?? null,
          timestamp: f?.timestamp ?? null,
          running,
          total,
        });
        lastKey = key;
      }
    }
    return events;
  }

  // ── Toast ──

  showError(message: string) {
    this.errorMessage = message;
    this.errorVisible = true;
    if (this.errorTimer) clearTimeout(this.errorTimer);
    this.errorTimer = setTimeout(() => (this.errorVisible = false), 5000);
  }

  // ── Frame index plumbing ──

  clampFrameIndex(index: number): number {
    if (!this.frames.length) return 0;
    return Math.max(0, Math.min(index, this.frames.length - 1));
  }

  /** Reset cursor + slider + display to "haven't loaded anything yet."
   *  Frames buffer is owned by the caller (each subclass refills it
   *  from its own source). */
  protected resetPlaybackCursor() {
    this.currentFrameIdx = 0;
    this.sliderValue = 0;
    this.sliderRequestedIdx = 0;
    this.displayFrame = null;
    this.selectedPod = null;
    this.selectedGangIdx = null;
  }

  async requestFrame(index: number) {
    if (!this.frames.length) return;
    const target = this.clampFrameIndex(Number(index));
    if (this.frameBusy) {
      this.queuedFrameIndex = target;
      this.frameRequestId += 1;
      return;
    }
    this.frameBusy = true;
    try {
      await this.setFrame(target);
    } finally {
      this.frameBusy = false;
      const queued = this.queuedFrameIndex;
      this.queuedFrameIndex = null;
      if (queued !== null && queued !== this.currentFrameIdx)
        this.requestFrame(queued);
    }
  }

  async setFrame(index: number) {
    if (!this.frames.length) return;
    const requestId = ++this.frameRequestId;
    this.currentFrameIdx = this.clampFrameIndex(index);
    if (!this.sliderDragging) {
      this.sliderValue = this.currentFrameIdx;
      this.sliderRequestedIdx = this.currentFrameIdx;
    }
    await this.loadAndDisplay(this.currentFrameIdx, requestId);
    if (requestId !== this.frameRequestId) return;
    this.syncRoute();
  }

  // ── Playback timer ──

  togglePlay() {
    if (!this.frames.length) return;
    this.playing = !this.playing;
    if (!this.playing) return;
    if (this.currentFrameIdx >= this.frames.length - 1) this.requestFrame(0);
    let last = performance.now();
    let advancing = false;
    const loop = (now: number) => {
      if (!this.playing) return;
      if (!advancing && now - last >= 1000 / this.fps) {
        last = now;
        if (this.currentFrameIdx >= this.frames.length - 1) {
          this.playing = false;
          return;
        }
        advancing = true;
        Promise.resolve(this.requestFrame(this.currentFrameIdx + 1)).finally(
          () => (advancing = false),
        );
      }
      requestAnimationFrame(loop);
    };
    requestAnimationFrame(loop);
  }

  stepPrev() {
    this.playing = false;
    this.requestFrame(this.currentFrameIdx - 1);
  }

  stepNext() {
    this.playing = false;
    this.requestFrame(this.currentFrameIdx + 1);
  }

  // ── Slider ──

  handleSliderInput(value: number) {
    this.sliderValue = value;
    this.sliderRequestedIdx = value;
    this.playing = false;
    if (this.sliderInputTimer) clearTimeout(this.sliderInputTimer);
    if (!this.frames.length) return;
    const delay = this.sliderDebounceMs();
    this.sliderInputTimer = setTimeout(() => this.requestFrame(value), delay);
  }

  /** How long to wait after the user stops dragging before triggering
   *  a load.  Live = 0 (cheap buffer read), replay with solver = 90ms
   *  (solver isn't free). */
  protected sliderDebounceMs(): number {
    return 0;
  }

  handleSliderPointerDown() {
    this.sliderDragging = true;
  }

  handleSliderPointerUp() {
    if (!this.sliderDragging) return;
    this.sliderDragging = false;
    if (this.frames.length) this.sliderValue = this.sliderRequestedIdx;
  }

  // ── URL syncing ──

  private _syncRouteTimer: ReturnType<typeof setTimeout> | null = null;

  syncRoute() {
    if (this._syncRouteTimer) return;
    this._syncRouteTimer = setTimeout(() => {
      this._syncRouteTimer = null;
      this.writeRoute();
    }, 200);
  }
}
