// Live cluster view: polls a k8s-bridge `/snapshot` proxy at fixed
// intervals, buffers the last LIVE_MAX_FRAMES, and exposes the rolling
// window through PlaybackCore.

import { fetchJson } from "./api";
import { PlaybackCore, type SummaryStats } from "./playback.svelte";
import type { Frame } from "./types";

const LIVE_MAX_FRAMES = 100;
const POLL_INTERVAL_MS = 500;

export type BridgeSource = {
  name: string;
  label: string;
  shortLabel?: string;
};

export class LiveState extends PlaybackCore {
  // Source the dropdown is pointed at.  Defaults to "milp" so the
  // local-dev `loop-runner` setup (which writes latest-milp.json) Just
  // Works; production overrides via /api/sources.
  liveSource = $state("milp");
  liveSources = $state<BridgeSource[]>([]);

  // Connection indicator surfaced in the header.
  connectionText = $state("Disconnected");
  connectionKind = $state("");

  // When true (default), advancing the buffer auto-scrolls to the
  // latest frame.  Turned off by a slider drag.
  autoFollow = $state(true);

  livePollTimer: ReturnType<typeof setInterval> | null = null;
  liveLastSeq = 0;

  // Source label for the header badge: prefer the terse `shortLabel`
  // when supplied (header is space-constrained), otherwise fall back
  // to the verbose `label` used in the dropdown.
  sourceLabel = $derived.by(() => {
    const found = this.liveSources.find((s) => s.name === this.liveSource);
    return found?.shortLabel || found?.label || this.liveSource;
  });

  // ── PlaybackCore seam ──

  async loadAndDisplay(idx: number, requestId: number): Promise<void> {
    const frame = this.frames[idx];
    if (requestId !== this.frameRequestId) return;
    this.displayFrame = frame;
  }

  getHistoricalFrame(idx: number): Frame | null {
    return this.frames[idx] ?? null;
  }

  tickLabel(_idx: number, frame: Frame | null): string {
    return `seq ${frame?.seq ?? 0}`;
  }

  summary(): SummaryStats {
    const f = this.displayFrame;
    if (!f) return { running: 0, queued: 0, utilization: 0 };
    return {
      running: f.summary?.running_jobs || 0,
      queued: f.summary?.queued_jobs || 0,
      utilization: f.summary?.utilization_percent || 0,
    };
  }

  protected writeRoute() {
    const params = new URLSearchParams();
    params.set("source", this.liveSource);
    if (this.frames.length) params.set("frame", String(this.currentFrameIdx));
    const url = new URL(window.location.href);
    url.search = params.toString();
    window.history.replaceState({}, "", url);
  }

  // ── Source list ──

  async loadLiveSources() {
    try {
      const data = await fetchJson("/api/sources");
      this.liveSources = Array.isArray(data) ? data : [];
    } catch {
      this.liveSources = [];
    }
    if (this.liveSources.length) {
      this.liveSource = this.liveSources[0].name;
      return;
    }
    // Dev fallback: BRIDGE_SOURCES isn't configured, so use whatever
    // /api/solvers reports as the seed for the file-backed
    // latest-<solver>.json that loop-runner writes.
    try {
      const solvers = await fetchJson("/api/solvers");
      if (Array.isArray(solvers) && solvers.length) {
        this.liveSource = solvers[0].ref || solvers[0].name || "milp";
      }
    } catch {
      // Stick with the constructor default.
    }
  }

  // ── Live polling ──

  async bootstrapLive(frame: number | null = null) {
    this.disconnectLive();
    this.connectionText = "Connecting...";
    this.connectionKind = "";
    try {
      const latest = await fetchJson(
        `/state/latest-${encodeURIComponent(this.liveSource)}.json`,
      );
      this.frames = [latest];
      this.liveLastSeq = latest.seq || 0;
    } catch {
      this.frames = [];
      this.showError(`No live data yet for ${this.liveSource}.`);
    }
    this.resetPlaybackCursor();
    const targetFrame =
      frame === null ? Math.max(this.frames.length - 1, 0) : Number(frame);
    await this.requestFrame(targetFrame);
    this.connectionText = "Connected";
    this.connectionKind = "live";
    this.livePollTimer = setInterval(
      () => this.pollLiveSnapshot(),
      POLL_INTERVAL_MS,
    );
  }

  async pollLiveSnapshot() {
    try {
      const snap = await fetchJson(
        `/state/latest-${encodeURIComponent(this.liveSource)}.json`,
      );
      const seq = snap?.seq || 0;
      if (seq > this.liveLastSeq) {
        this.liveLastSeq = seq;
        this.upsertLiveSnapshot(snap);
      }
    } catch {
      this.connectionText = "Reconnecting...";
      this.connectionKind = "error";
    }
  }

  upsertLiveSnapshot(snapshot: Frame) {
    const existingIndex = this.frames.findIndex((f) => f.seq === snapshot.seq);
    if (existingIndex >= 0) {
      this.frames[existingIndex] = snapshot;
    } else {
      this.frames.push(snapshot);
      this.frames.sort((a, b) => (a.seq || 0) - (b.seq || 0));
    }
    if (this.frames.length > LIVE_MAX_FRAMES) {
      const excess = this.frames.length - LIVE_MAX_FRAMES;
      this.frames.splice(0, excess);
      this.currentFrameIdx = Math.max(0, this.currentFrameIdx - excess);
    }
    if (this.autoFollow || this.currentFrameIdx >= this.frames.length - 2) {
      this.requestFrame(this.frames.length - 1);
    } else {
      this.displayFrame = this.frames[this.currentFrameIdx];
      this.syncRoute();
    }
  }

  disconnectLive() {
    if (this.livePollTimer) {
      clearInterval(this.livePollTimer);
      this.livePollTimer = null;
    }
    this.connectionText = "Disconnected";
    this.connectionKind = "";
  }

  onLiveSourceChange() {
    this.bootstrapLive().catch((e: any) => this.showError(e.message));
  }

  // ── Init ──

  async initFromUrl() {
    await this.loadLiveSources();
    const params = new URLSearchParams(window.location.search);
    const src = params.get("source") || params.get("scheduler");
    if (src) this.liveSource = src;
    const frame = params.has("frame") ? Number(params.get("frame")) : null;
    try {
      await this.bootstrapLive(frame);
    } catch (e: any) {
      this.showError(e.message);
    }
  }
}
