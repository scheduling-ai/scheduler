export interface Replica {
  node: string | null;
  phase: string;
}

export interface Pod {
  priority: number;
  quota: string;
  chip_type: string;
  chips_per_replica: number;
  statuses_by_replica: Replica[];
}

export interface Node {
  name: string;
  chips: number;
  chip_type: string;
}

export interface Cluster {
  name: string;
  nodes: Node[];
}

export interface Quota {
  name: string;
  guarantees: Record<string, Record<string, number>>;
}

export interface Frame {
  clusters?: Cluster[];
  pods?: Record<string, Pod>;
  gang_sets?: string[][];
  quotas?: Quota[];
  seq?: number;
  timestamp?: string;
  reason?: string;
  summary?: {
    running_jobs: number;
    queued_jobs: number;
    utilization_percent: number;
  };
  solver_status?: string;
  solver_duration_ms?: number;
  scheduler?: string;
}

// ── Small-scale types (chip-level rendering) ──

export interface Alloc {
  pod: string;
  phase: string;
  chips: number;
}

export interface Segment {
  gangIdx: number;
  multiPod: boolean;
  chipCount: number;
  allocs: Alloc[];
}

export interface ParsedNode {
  name: string;
  chipType: string;
  capacity: number;
  used: number;
  segments: Segment[];
  free: number;
}

export interface ParsedCluster {
  name: string;
  nodes: ParsedNode[];
}

export interface QueueItem {
  pod: string;
  priority: number;
  quota: string;
  chipType: string;
  chips: number;
  queued: number;
}

export interface GangInfo {
  podToIdx: Map<string, number>;
  gangSetMembers: Map<number, Set<string>>;
}

// ── Scale-aware types ──

export interface Job {
  name: string;
  totalGpus: number;
  replicas: number;
  chipsPerReplica: number;
  priority: number;
  quota: string;
  chipType: string;
  status: "running" | "partial" | "pending" | "suspended";
  placedReplicas: number;
  gangIdx: number | null;
  gangMembers: string[];
  borrowing: boolean;
}

export interface DeploymentGroup {
  id: string;
  prefix: string;
  quota: string;
  chipType: string;
  priority: number;
  running: number;
  pending: number;
  total: number;
  clusterCounts: Record<string, number>;
  borrowing: boolean;
}

export type Workload =
  | ({ kind: "job" } & Job)
  | ({ kind: "deployment" } & DeploymentGroup);

export interface JobHistoryEvent {
  frame: number;
  status: string;
}

export interface ChipFree {
  chipType: string;
  free: number;
  total: number;
}

export interface ClusterChipFree {
  cluster: string;
  chipType: string;
  free: number;
  total: number;
}

export interface QuotaSummary {
  name: string;
  chipBreakdown: {
    chipType: string;
    guaranteed: number;
    used: number;
  }[];
  workloads: Workload[];
  clusterFree: ClusterChipFree[];
}

export interface QueueEntry {
  kind: "job" | "deployment-rollup";
  name: string;
  priority: number;
  quota: string;
  chipType: string;
  chips: number;
  count: number;
}

export interface ParsedView {
  clusters: ParsedCluster[];
  queue: QueueItem[];
  gangInfo: GangInfo;
  totalNodes: number;
  utilization: number;
  isLargeScale: boolean;
  chipFree: ChipFree[];
  clusterChipFree: ClusterChipFree[];
  quotaSummaries: QuotaSummary[];
  jobs: Job[];
  deployments: DeploymentGroup[];
  workloads: Workload[];
  queueEntries: QueueEntry[];
}
