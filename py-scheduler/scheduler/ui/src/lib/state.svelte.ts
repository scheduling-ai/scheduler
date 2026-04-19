import { fetchJson } from "./api";
import { parseFrame } from "./parse";
import { GeneratorState } from "./generator.svelte";
import type { Frame } from "./types";

const LIVE_MAX_FRAMES = 100;

class SimState {
  frames = $state<Frame[]>([]);
  currentFrameIdx = $state(0);
  displayFrame = $state<Frame | null>(null);
  playing = $state(false);
  fps = $state(5);
  currentMode = $state<"none" | "replay" | "live">("none");
  currentSource = $state<"none" | "live" | "scenario" | "file">("none");
  currentScenarioName = $state("production_scale");
  currentSessionUrl = $state("");
  liveScheduler = $state("milp");
  replayRunSolver = $state(true);
  replaySolver = $state("milp");
  homeLiveScheduler = $state("milp");
  homeScenarioSolver = $state("milp");
  selectedPod = $state<string | null>(null);
  selectedGangIdx = $state<number | null>(null);
  selectedQuota = $state<string | null>(null);
  selectedWorkload = $state<string | null>(null);
  selectedChipType = $state<string | null>(null);
  selectedCluster = $state<string | null>(null);
  globalSearch = $state("");
  solvedFrames = $state<Record<number, Frame>>({});
  frameBusy = $state(false);
  sliderDragging = $state(false);
  sliderValue = $state(0);
  homeVisible = $state(true);
  generatorOpen = $state(false);
  helpOpen = $state(false);
  autoFollow = $state(true);
  connectionText = $state("Disconnected");
  connectionKind = $state("");
  errorMessage = $state("");
  errorVisible = $state(false);
  scenarios = $state<{ name: string; description: string }[]>([]);
  solvers = $state<{ name: string; ref: string }[]>([]);
  gen = new GeneratorState(
    () => this.currentMode === "live",
    (msg) => this.showError(msg),
  );

  livePollTimer: ReturnType<typeof setInterval> | null = null;
  liveLastSeq = 0;
  frameRequestId = 0;
  queuedFrameIndex: number | null = null;
  sliderRequestedIdx = 0;
  sliderInputTimer: ReturnType<typeof setTimeout> | null = null;
  errorTimer: ReturnType<typeof setTimeout> | null = null;
  playAnimId: number | null = null;

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

  // ── Methods ──

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
        if (dep) {
          this.selectedQuota = dep.quota;
        }
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
    let lastStatus = "absent";
    for (let i = 0; i < this.frames.length; i++) {
      const f = this.frames[i];
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
      if (i === 0 || status !== lastStatus) {
        events.push({
          frame: i,
          seq: f?.seq ?? null,
          timestamp: f?.timestamp ?? null,
          status,
        });
        lastStatus = status;
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
    for (let i = 0; i < this.frames.length; i++) {
      const f = this.frames[i];
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
      const key = `${running}/${total}`;
      if (i === 0 || key !== lastKey) {
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

  showError(message: string) {
    this.errorMessage = message;
    this.errorVisible = true;
    if (this.errorTimer) clearTimeout(this.errorTimer);
    this.errorTimer = setTimeout(() => (this.errorVisible = false), 5000);
  }

  clampFrameIndex(index: number): number {
    if (!this.frames.length) return 0;
    return Math.max(0, Math.min(index, this.frames.length - 1));
  }

  private _syncRouteTimer: ReturnType<typeof setTimeout> | null = null;

  routeState() {
    const params = new URLSearchParams();
    let path = "/";
    if (this.currentSource === "live") {
      path = "/live";
      params.set("scheduler", this.liveScheduler);
      if (this.frames.length) params.set("frame", String(this.currentFrameIdx));
    } else if (this.currentSource === "scenario" && this.currentScenarioName) {
      path = `/scenarios/${encodeURIComponent(this.currentScenarioName)}`;
      params.set("solver", this.replaySolver);
      if (this.frames.length) params.set("frame", String(this.currentFrameIdx));
    } else if (this.currentSource === "file") {
      path = "/replay";
      params.set("solver", this.replaySolver);
      params.set("run_solver", this.replayRunSolver ? "1" : "0");
      if (this.currentSessionUrl) params.set("session", this.currentSessionUrl);
      if (this.frames.length) params.set("frame", String(this.currentFrameIdx));
    }
    return { path, params };
  }

  syncRoute() {
    if (this._syncRouteTimer) return;
    this._syncRouteTimer = setTimeout(() => {
      this._syncRouteTimer = null;
      const url = new URL(window.location.href);
      const route = this.routeState();
      url.pathname = route.path;
      url.search = route.params.toString();
      window.history.replaceState({}, "", url);
    }, 200);
  }

  async loadScenarios() {
    const data = await fetchJson("/scenarios/index.json");
    this.scenarios = data;
  }
  async loadSolvers() {
    const data = await fetchJson("/api/solvers");
    this.solvers = data;
    this.replaySolver = "milp";
    this.liveScheduler = "milp";
    this.homeLiveScheduler = "milp";
    this.homeScenarioSolver = "milp";
  }
  disconnectLive() {
    if (this.livePollTimer) {
      clearInterval(this.livePollTimer);
      this.livePollTimer = null;
    }
    if (this.currentMode !== "live") {
      this.connectionText = "Disconnected";
      this.connectionKind = "";
    }
  }
  resetAppState() {
    this.frames = [];
    this.currentFrameIdx = 0;
    this.playing = false;
    this.solvedFrames = {};
    this.selectedPod = null;
    this.selectedGangIdx = null;
    this.frameBusy = false;
    this.queuedFrameIndex = null;
    this.sliderValue = 0;
    this.sliderRequestedIdx = 0;
    this.displayFrame = null;
  }
  openHome(updateRoute = true) {
    this.disconnectLive();
    this.currentMode = "none";
    this.currentSource = "none";
    this.currentScenarioName = "";
    this.currentSessionUrl = "";
    this.resetAppState();
    if (updateRoute) this.syncRoute();
    this.homeVisible = true;
    this.connectionText = "Disconnected";
    this.connectionKind = "";
  }
  initApp(mode: "replay" | "live") {
    this.currentMode = mode;
    this.homeVisible = false;
    this.solvedFrames = {};
    this.selectedPod = null;
    this.selectedGangIdx = null;
    this.currentFrameIdx = 0;
    this.sliderValue = 0;
    this.sliderRequestedIdx = 0;
    this.displayFrame = null;
    if (mode === "live" && !this.gen.polling) this.gen.startPolling();
    if (mode !== "live") this.generatorOpen = false;
  }
  async loadScenario(
    options: { name?: string; solver?: string; frame?: number } = {},
  ) {
    this.disconnectLive();
    const name = options.name || this.scenarios[0]?.name || "gang_scheduling";
    const solver = options.solver || this.homeScenarioSolver || "milp";
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
    this.currentSource = "scenario";
    this.currentScenarioName = name;
    this.currentSessionUrl = "";
    this.replaySolver = solver;
    const podCount = Object.keys(data[0]?.pods || {}).length;
    this.replayRunSolver = podCount < 500;
    this.initApp("replay");
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
    this.disconnectLive();
    this.frames = parsed;
    this.currentSource = "file";
    this.currentScenarioName = "";
    this.currentSessionUrl = routeParams.session || "";
    this.replaySolver = routeParams.solver || this.replaySolver || "milp";
    this.replayRunSolver = routeParams.runSolver ?? true;
    this.initApp("replay");
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
  async bootstrapLive(frame: number | null = null) {
    const scheduler = this.liveScheduler;
    this.disconnectLive();
    this.currentSource = "live";
    this.currentScenarioName = "";
    this.currentSessionUrl = "";
    this.connectionText = "Connecting...";
    this.connectionKind = "";
    try {
      const latest = await fetchJson(
        `/state/latest-${encodeURIComponent(scheduler)}.json`,
      );
      this.frames = [latest];
      this.liveLastSeq = latest.seq || 0;
    } catch {
      this.frames = [];
      this.showError(`No live data yet for ${scheduler}.`);
    }
    this.initApp("live");
    this.syncRoute();
    const targetFrame =
      frame === null ? Math.max(this.frames.length - 1, 0) : Number(frame);
    await this.requestFrame(targetFrame);
    this.connectionText = "Connected";
    this.connectionKind = "live";
    this.livePollTimer = setInterval(() => this.pollLiveSnapshot(), 500);
  }
  async pollLiveSnapshot() {
    if (this.currentMode !== "live") return;
    try {
      const scheduler = encodeURIComponent(this.liveScheduler);
      const snap = await fetchJson(`/state/latest-${scheduler}.json`);
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
    if (
      this.currentMode !== "live" ||
      snapshot.scheduler !== this.liveScheduler
    )
      return;
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
  async solveFrame(frame: Frame) {
    const started = performance.now();
    const solver = encodeURIComponent(this.replaySolver.trim());
    const solved = await fetchJson(`/api/solve?solver=${solver}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(frame),
    });
    if (typeof solved.solver_duration_ms !== "number")
      solved.solver_duration_ms = Math.round(performance.now() - started);
    return solved;
  }
  buildWorldState(index: number): Frame {
    const raw = this.frames[index];
    if (index === 0 || !this.solvedFrames[index - 1]) return raw;
    const previous = this.solvedFrames[index - 1];
    const mergedPods: Record<string, any> = {};
    for (const [name, pod] of Object.entries(raw.pods || {}))
      mergedPods[name] = previous.pods?.[name] || pod;
    return { ...raw, pods: mergedPods };
  }
  async ensureSolvedUpTo(index: number, requestId: number): Promise<boolean> {
    for (let i = 0; i < index; i++) {
      if (requestId !== this.frameRequestId) return false;
      if (this.solvedFrames[i]) continue;
      const world = this.buildWorldState(i);
      const solved = await this.solveFrame(world);
      if (requestId !== this.frameRequestId) return false;
      this.solvedFrames[i] = {
        ...world,
        pods: solved.pods,
        solver_status: solved.solver_status || "ok",
        solver_duration_ms: solved.solver_duration_ms ?? null,
      };
    }
    return true;
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
    if (this.currentMode === "live") {
      const frame = this.frames[this.currentFrameIdx];
      if (requestId !== this.frameRequestId) return;
      this.displayFrame = frame;
      this.syncRoute();
      return;
    }
    const raw = this.frames[this.currentFrameIdx];
    if (!this.replayRunSolver) {
      if (requestId !== this.frameRequestId) return;
      this.displayFrame = raw;
      this.syncRoute();
      return;
    }
    let world: Frame = raw;
    try {
      if (
        this.currentFrameIdx > 0 &&
        !this.solvedFrames[this.currentFrameIdx - 1]
      ) {
        const caughtUp = await this.ensureSolvedUpTo(
          this.currentFrameIdx,
          requestId,
        );
        if (!caughtUp || requestId !== this.frameRequestId) return;
      }
      world = this.buildWorldState(this.currentFrameIdx);
      if (requestId !== this.frameRequestId) return;
      this.displayFrame = world;
      const solved = await this.solveFrame(world);
      if (requestId !== this.frameRequestId) return;
      this.solvedFrames[this.currentFrameIdx] = {
        ...world,
        pods: solved.pods,
        solver_status: solved.solver_status || "ok",
        solver_duration_ms: solved.solver_duration_ms ?? null,
      };
      this.solvedFrames = { ...this.solvedFrames };
    } catch (error: any) {
      if (requestId !== this.frameRequestId) return;
      this.showError(error.message);
    }
    this.syncRoute();
  }
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
  handleSliderInput(value: number) {
    this.sliderValue = value;
    this.sliderRequestedIdx = value;
    this.playing = false;
    if (this.sliderInputTimer) clearTimeout(this.sliderInputTimer);
    if (!this.frames.length) return;
    const delay =
      this.currentMode === "replay" && this.replayRunSolver ? 90 : 0;
    this.sliderInputTimer = setTimeout(() => this.requestFrame(value), delay);
  }
  handleSliderPointerDown() {
    this.sliderDragging = true;
  }
  handleSliderPointerUp() {
    if (!this.sliderDragging) return;
    this.sliderDragging = false;
    if (this.frames.length) this.sliderValue = this.sliderRequestedIdx;
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
  onReplaySolverChange() {
    this.solvedFrames = {};
    if (
      this.currentMode === "replay" &&
      this.replayRunSolver &&
      this.frames.length
    )
      this.requestFrame(this.currentFrameIdx);
    else this.syncRoute();
  }
  onReplayRunSolverChange(checked: boolean) {
    this.replayRunSolver = checked;
    this.solvedFrames = {};
    if (this.currentMode === "replay" && this.frames.length)
      this.requestFrame(this.currentFrameIdx);
    else this.syncRoute();
  }
  onLiveSchedulerChange() {
    if (this.currentMode === "live")
      this.bootstrapLive().catch((e: any) => this.showError(e.message));
  }
  async initFromUrl() {
    await Promise.all([this.loadScenarios(), this.loadSolvers()]);
    const params = new URLSearchParams(window.location.search);
    const pathname = decodeURIComponent(
      window.location.pathname.replace(/\/+$/, "") || "/",
    );
    if (pathname === "/replay" && params.get("session")) {
      try {
        await this.loadUrl(params.get("session")!, {
          solver: params.get("solver") || "milp",
          runSolver: params.get("run_solver") === "1",
          frame: Number(params.get("frame") || 0),
        });
        return;
      } catch (error: any) {
        this.showError(error.message);
      }
    }
    if (pathname === "/live" || params.get("mode") === "live") {
      try {
        if (params.get("scheduler"))
          this.liveScheduler = params.get("scheduler")!;
        await this.bootstrapLive(Number(params.get("frame") || 0));
        return;
      } catch (error: any) {
        this.showError(error.message);
      }
    }
    if (
      pathname.startsWith("/scenarios/") ||
      params.get("mode") === "scenario"
    ) {
      const scenario = pathname.startsWith("/scenarios/")
        ? pathname.split("/").pop()!
        : params.get("scenario");
      if (scenario) {
        try {
          await this.loadScenario({
            name: scenario,
            solver: params.get("solver") || "milp",
            frame: Number(params.get("frame") || 0),
          });
          return;
        } catch (error: any) {
          this.showError(error.message);
        }
      }
    }
    if (params.get("session")) {
      try {
        await this.loadUrl(params.get("session")!, {
          solver: params.get("solver") || "milp",
          runSolver: params.get("run_solver") === "1",
          frame: Number(params.get("frame") || 0),
        });
        return;
      } catch (error: any) {
        this.showError(error.message);
      }
    }
  }
}

export const sim = new SimState();
