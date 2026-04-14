import { fetchJson } from "./api";

export class GeneratorState {
  connected = $state(false);
  running = $state(false);
  formDirty = $state(false);
  form = $state({
    seed: "7",
    arrival_rate: "0.15",
    burst_factor: "1.4",
    loop_interval_seconds: "5",
    priority_min: "30",
    priority_max: "99",
    replica_min: "1",
    replica_max: "2",
    runtime_min: "12",
    runtime_max: "40",
    gang_frequency: "0.08",
    replica_failure_rate: "0.03",
    node_failure_rate: "0.005",
    node_recovery_rate: "0.03",
    quota_weights: '{"training": 1, "research": 1, "inference": 1}',
    chip_weights: '{"H200": 1, "H100": 1, "A100": 1, "L40S": 0.7}',
    chips_weights: '{"1": 0.2, "2": 0.25, "4": 0.3, "8": 1}',
  });

  polling = false;
  pollTimer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private isLive: () => boolean,
    private onError: (msg: string) => void,
  ) {}

  fillForm(config: any) {
    this.form = {
      seed: String(config.seed),
      arrival_rate: String(config.arrival_rate),
      burst_factor: String(config.burst_factor),
      loop_interval_seconds: String(config.loop_interval_seconds),
      priority_min: String(config.priority_min),
      priority_max: String(config.priority_max),
      replica_min: String(config.replica_min),
      replica_max: String(config.replica_max),
      runtime_min: String(config.runtime_min),
      runtime_max: String(config.runtime_max),
      gang_frequency: String(config.gang_frequency),
      replica_failure_rate: String(config.replica_failure_rate),
      node_failure_rate: String(config.node_failure_rate),
      node_recovery_rate: String(config.node_recovery_rate),
      quota_weights: JSON.stringify(config.quota_weights, null, 2),
      chip_weights: JSON.stringify(config.chip_weights, null, 2),
      chips_weights: JSON.stringify(config.chips_weights, null, 2),
    };
    this.formDirty = false;
  }

  formPayload() {
    return {
      seed: Number(this.form.seed),
      arrival_rate: Number(this.form.arrival_rate),
      burst_factor: Number(this.form.burst_factor),
      loop_interval_seconds: Number(this.form.loop_interval_seconds),
      priority_min: Number(this.form.priority_min),
      priority_max: Number(this.form.priority_max),
      replica_min: Number(this.form.replica_min),
      replica_max: Number(this.form.replica_max),
      runtime_min: Number(this.form.runtime_min),
      runtime_max: Number(this.form.runtime_max),
      gang_frequency: Number(this.form.gang_frequency),
      replica_failure_rate: Number(this.form.replica_failure_rate),
      node_failure_rate: Number(this.form.node_failure_rate),
      node_recovery_rate: Number(this.form.node_recovery_rate),
      quota_weights: JSON.parse(this.form.quota_weights),
      chip_weights: JSON.parse(this.form.chip_weights),
      chips_weights: JSON.parse(this.form.chips_weights),
    };
  }

  async refresh() {
    try {
      const config = await fetchJson("/state/config.json");
      this.connected = true;
      this.running = config.running ?? true;
      if (!this.formDirty) this.fillForm(config);
    } catch {
      this.connected = false;
    }
  }

  startPolling() {
    if (this.polling) return;
    this.polling = true;
    this.refresh();
    this.pollTimer = setInterval(() => {
      if (this.isLive()) this.refresh();
    }, 2000);
  }

  private async action(fn: () => Promise<void>) {
    try {
      await fn();
    } catch (error: any) {
      this.onError(error.message);
    }
  }

  async setRunning(running: boolean) {
    await this.action(async () => {
      await fetchJson("/api/generator/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ running }),
      });
      await this.refresh();
    });
  }

  start() {
    return this.setRunning(true);
  }
  pause() {
    return this.setRunning(false);
  }
  resume() {
    return this.setRunning(true);
  }

  async saveConfig() {
    await this.action(async () => {
      const payload = this.formPayload();
      await fetchJson("/api/generator/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      this.formDirty = false;
      await this.refresh();
    });
  }
}
