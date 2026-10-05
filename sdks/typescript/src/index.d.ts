// Type declarations for the db2graph-sync service SDK (management API v1).

export type ErrorCode =
  | "config" | "preflight" | "feature_not_compiled" | "restricted_provider_unavailable" | "security" | "secret"
  | "state_tampered" | "state_rollback" | "state_io" | "ownership_conflict" | "fenced_out" | "source"
  | "source_identity_changed" | "resume_history_lost" | "schema_incompatible" | "mapping_not_approved" | "graph"
  | "consistency_incident" | "vector" | "audit_unavailable" | "limit_exceeded" | "timeout" | "shutdown"
  | "invalid_request" | "unauthorized" | "unsupported" | "internal";

export class D2GError extends Error {
  code: ErrorCode | string;
  httpStatus: number;
  correlationId: string;
}

export type Phase = "starting" | "preflight" | "recovering" | "snapshotting" | "validating" | "streaming" | "paused" | "draining" | "stopped" | "failed";

export interface Status {
  pipeline: string;
  profile: "development" | "enterprise" | "restricted";
  phase: Phase;
  ready: boolean;
  pause_reason: string | null;
  failure: { kind: ErrorCode; message: string } | null;
  delivery_guarantee: "best_effort" | "at_least_once" | "effectively_once_effects";
  [key: string]: unknown;
}

export interface Principal {
  tenant: string;
  classifications?: string[];
  cui_categories?: string[];
  export_authorizations?: string[];
  releasability_code?: string | null;
}

export interface Hit { entity_id: string; version: string }

export interface RequestOptions { idempotencyKey?: string; correlationId?: string }

export class Client {
  constructor(baseUrl: string, opts?: { token?: string; fetch?: typeof fetch; timeoutMs?: number; allowInsecureHttp?: boolean });
  about(): Promise<Record<string, unknown>>;
  ready(): Promise<boolean>;
  status(opts?: RequestOptions): Promise<Status>;
  metrics(opts?: RequestOptions): Promise<string>;
  pause(opts?: RequestOptions): Promise<{ ok: boolean; phase: Phase }>;
  resume(opts?: RequestOptions): Promise<{ ok: boolean; phase: Phase }>;
  approveMapping(digest: string, opts?: RequestOptions): Promise<{ ok: boolean; phase: Phase }>;
  validateRetrieval(principal: Principal, hits: Hit[], versionRule?: "exact_version" | "any_version", opts?: RequestOptions): Promise<{ verdicts: unknown[] }>;
}
