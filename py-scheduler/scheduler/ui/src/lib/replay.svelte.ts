// Replay / scenario view: loads frames from a JSONL (file, URL, or
// built-in scenario), then walks the solver chain forward as the user
// scrubs — solving frame i requires having solved frames 0..i-1
// because each step's input is the previous step's output.
//
// Solved frames are cached in `solvedFrames` keyed by index; the
// solver itself runs server-side at /api/solve.

import { fetchJson } from "./api";
import { PlaybackCore, type SummaryStats } from "./playback.svelte";
import type { Frame } from "./types";

export type Scenario = { name: string; description: string };
export type SolverRef = { name: string; ref: string };

export class ReplayState extends PlaybackCore {
  // What we're replaying.  Exactly one of (scenarioName, sessionUrl)
  // is meaningful at a time; the other is "".
  currentScenarioName = $state("");
  currentSessionUrl = $state("");

  // Solver controls.  replayRunSolver=false short-circuits the chain
  // and just displays raw frames (useful for big scenarios where the
  // solver is slow).
  replayRunSolver = $state(true);
  replaySolver = $state("milp");

  scenarios = $state<Scenario[]>([]);
  solvers = $state<SolverRef[]>([]);

  // Cache of solver output keyed by frame index.  Cleared on
  // load / solver-change / replayRunSolver toggle.
  solvedFrames = $state<Record<number, Frame>>({});

  // ── PlaybackCore seam ──

  async loadAndDisplay(idx: number, requestId: number): Promise<void> {
    const raw = this.frames[idx];
    if (!this.replayRunSolver) {
      if (requestId !== this.frameRequestId) return;
      this.displayFrame = raw;
      return;
    }
    try {
      if (idx > 0 && !this.solvedFrames[idx - 1]) {
        const caughtUp = await this.ensureSolvedUpTo(idx, requestId);
        if (!caughtUp || requestId !== this.frameRequestId) return;
      }
      const world = this.buildWorldState(idx);
      if (requestId !== this.frameRequestId) return;
      this.displayFrame = world;
      const solved = await this.solveFrame(world, idx);
      if (requestId !== this.frameRequestId) return;
      this.solvedFrames[idx] = {
        ...world,
        pods: solved.pods,
        solver_status: solved.solver_status || "ok",
        solver_duration_ms: solved.solver_duration_ms ?? undefined,
      };
      this.solvedFrames = { ...this.solvedFrames };
    } catch (error: any) {
      if (requestId !== this.frameRequestId) return;
      this.showError(`Frame ${idx}: ${error.message}`);
    }
  }

  getHistoricalFrame(idx: number): Frame | null {
    if (idx === 0) return this.frames[0] ?? null;
    if (!this.solvedFrames[idx - 1]) return null;
    return this.buildWorldState(idx);
  }

  tickLabel(idx: number, _frame: Frame | null): string {
    return `t=${idx}`;
  }

  summary(): SummaryStats {
    const pv = this.parsedView;
    if (!pv) return { running: 0, queued: 0, utilization: 0 };
    const running = pv.clusters.reduce(
      (sum, c) =>
        sum +
        c.nodes.reduce(
          (ns, n) => ns + n.segments.reduce((ss, s) => ss + s.allocs.length, 0),
          0,
        ),
      0,
    );
    const queued = pv.queue.reduce((s, q) => s + q.queued, 0);
    return { running, queued, utilization: pv.utilization };
  }

  protected sliderDebounceMs(): number {
    // The solver runs synchronously per frame; debouncing avoids
    // queueing N solves while the user drags.
    return this.replayRunSolver ? 90 : 0;
  }

  protected writeRoute() {
    const params = new URLSearchParams();
    params.set("solver", this.replaySolver);
    params.set("run_solver", this.replayRunSolver ? "1" : "0");
    if (this.currentScenarioName)
      params.set("scenario", this.currentScenarioName);
    if (this.currentSessionUrl) params.set("session", this.currentSessionUrl);
    if (this.frames.length) params.set("frame", String(this.currentFrameIdx));
    const url = new URL(window.location.href);
    url.search = params.toString();
    window.history.replaceState({}, "", url);
  }

  // ── Catalogue loads ──

  async loadScenarios() {
    const data = await fetchJson("/scenarios/index.json");
    this.scenarios = data;
  }

  async loadSolvers() {
    const data = await fetchJson("/api/solvers");
    this.solvers = data;
  }

  // ── Source loads ──

  async loadScenario(
    options: { name?: string; solver?: string; frame?: number } = {},
  ) {
    const name = options.name || this.scenarios[0]?.name || "gang_scheduling";
    const solver = options.solver || this.replaySolver || "milp";
    const response = await fetch(
      `/scenarios/${encodeURIComponent(name)}.jsonl`,
    );
    if (!response.ok)
      throw new Error(`Failed to load scenario: ${response.statusText}`);
    const text = await response.text();
    const data = text
      .trim()
      .split("\n")
      .filter((line) => line)
      .map((line) => JSON.parse(line));
    this.frames = data;
    this.currentScenarioName = name;
    this.currentSessionUrl = "";
    this.replaySolver = solver;
    const podCount = Object.keys(data[0]?.pods || {}).length;
    this.replayRunSolver = podCount < 500;
    this.solvedFrames = {};
    this.resetPlaybackCursor();
    this.syncRoute();
    await this.requestFrame(Number(options.frame ?? 0));
  }

  async parseText(
    text: string,
    routeParams: {
      solver?: string;
      runSolver?: boolean;
      frame?: number;
      session?: string;
    } = {},
  ) {
    const parsed: Frame[] = [];
    for (const line of text.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        parsed.push(JSON.parse(trimmed));
      } catch {
        continue;
      }
    }
    if (!parsed.length) throw new Error("No valid JSONL lines found.");
    this.frames = parsed;
    this.currentScenarioName = "";
    this.currentSessionUrl = routeParams.session || "";
    this.replaySolver = routeParams.solver || this.replaySolver || "milp";
    this.replayRunSolver = routeParams.runSolver ?? true;
    this.solvedFrames = {};
    this.resetPlaybackCursor();
    this.syncRoute();
    await this.requestFrame(Number(routeParams.frame ?? 0));
  }

  async loadUrl(
    url: string,
    options: { solver?: string; runSolver?: boolean; frame?: number } = {},
  ) {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    await this.parseText(await response.text(), { ...options, session: url });
  }

  handleFile(file: File) {
    const reader = new FileReader();
    reader.onload = async (event) => {
      try {
        await this.parseText(event.target!.result as string);
      } catch (error: any) {
        this.showError(error.message);
      }
    };
    reader.readAsText(file);
  }

  // ── Solver chain ──

  async solveFrame(frame: Frame, frameIdx: number): Promise<Frame> {
    const started = performance.now();
    const solver = encodeURIComponent(this.replaySolver.trim());
    try {
      const solved = await fetchJson(`/api/solve?solver=${solver}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(frame),
      });
      if (typeof solved.solver_duration_ms !== "number")
        solved.solver_duration_ms = Math.round(performance.now() - started);
      const placed = Object.values(
        (solved.pods ?? {}) as Record<
          string,
          { statuses_by_replica?: { node?: string }[] }
        >,
      ).reduce(
        (n, p) =>
          n + (p.statuses_by_replica ?? []).filter((r) => r.node).length,
        0,
      );
      console.log(
        `[solve] frame=${frameIdx} solver=${this.replaySolver} status=${solved.solver_status ?? "ok"} duration=${solved.solver_duration_ms}ms placed=${placed}`,
      );
      return solved;
    } catch (error: any) {
      console.error(`[solve] frame=${frameIdx} failed:`, error);
      throw error;
    }
  }

  /** Merge the raw frame at `index` with the previous solve's pod
   *  assignments — the world state the solver should see as input
   *  for `index`. */
  buildWorldState(index: number): Frame {
    const raw = this.frames[index];
    if (index === 0 || !this.solvedFrames[index - 1]) return raw;
    const previous = this.solvedFrames[index - 1];
    const mergedPods: Record<string, any> = {};
    for (const [name, pod] of Object.entries(raw.pods || {}))
      mergedPods[name] = previous.pods?.[name] || pod;
    return { ...raw, pods: mergedPods };
  }

  /** Solve every uncached frame strictly before `index`, in order.
   *  Used when the user jumps to a frame whose prereqs aren't ready
   *  yet (slider seek). */
  async ensureSolvedUpTo(index: number, requestId: number): Promise<boolean> {
    for (let i = 0; i < index; i++) {
      if (requestId !== this.frameRequestId) return false;
      if (this.solvedFrames[i]) continue;
      const world = this.buildWorldState(i);
      const solved = await this.solveFrame(world, i);
      if (requestId !== this.frameRequestId) return false;
      this.solvedFrames[i] = {
        ...world,
        pods: solved.pods,
        solver_status: solved.solver_status || "ok",
        solver_duration_ms: solved.solver_duration_ms ?? undefined,
      };
    }
    return true;
  }

  /** Drop the cached solve for the current frame and re-trigger it.
   *  Useful when attaching a Python debugger to /api/solve — without
   *  this you'd need to reload the whole scenario to re-run a
   *  specific frame's solve. */
  async resolveCurrentFrame() {
    if (!this.replayRunSolver || !this.frames.length) return;
    const idx = this.currentFrameIdx;
    const next = { ...this.solvedFrames };
    delete next[idx];
    this.solvedFrames = next;
    await this.requestFrame(idx);
  }

  onReplaySolverChange() {
    this.solvedFrames = {};
    if (this.replayRunSolver && this.frames.length)
      this.requestFrame(this.currentFrameIdx);
    else this.syncRoute();
  }

  onReplayRunSolverChange(checked: boolean) {
    this.replayRunSolver = checked;
    this.solvedFrames = {};
    if (this.frames.length) this.requestFrame(this.currentFrameIdx);
    else this.syncRoute();
  }

  // ── Init ──

  async initFromUrl() {
    await Promise.all([this.loadScenarios(), this.loadSolvers()]);
    const params = new URLSearchParams(window.location.search);
    const session = params.get("session");
    const scenario = params.get("scenario");
    const solver = params.get("solver") || "milp";
    const runSolver = params.get("run_solver");
    const frame = Number(params.get("frame") || 0);

    if (session) {
      try {
        await this.loadUrl(session, {
          solver,
          runSolver: runSolver === null ? true : runSolver === "1",
          frame,
        });
        return;
      } catch (error: any) {
        this.showError(error.message);
      }
    }
    if (scenario) {
      try {
        await this.loadScenario({ name: scenario, solver, frame });
        return;
      } catch (error: any) {
        this.showError(error.message);
      }
    }
  }
}
