import { scoped } from "@accountability/evidence/database";
import type { Database, UnitOfWork } from "@accountability/evidence/database";
import {
  canonical,
  digest,
  DomainError,
  hashBytes,
  invariant,
  newId,
  nonempty,
  requireScope,
  validTime,
  validateRef,
} from "./contracts.ts";
import type { Context, Json, Ref, Scope } from "./contracts.ts";
import {
  createCollection,
  dueItems,
  startAcquisition,
  finishAcquisition,
  acquisitionKey,
} from "./collection.ts";
import type { Collection } from "./collection.ts";
import type { Capture, SourcePolicy } from "./index.ts";
export type AcquisitionLease = {
  token: string;
  attemptId: string;
  itemId: string;
  actorId: string;
  actorKind: Context["actor"]["kind"];
  leasedAt: string;
  expiresAt: string;
  receipt: Ref | null;
  status: "leased" | "running" | "reconcile" | "completed";
};
export type AcquisitionSchedule = {
  schemaVersion: 1;
  enabled: boolean;
  collection: Collection;
  leases: readonly AcquisitionLease[];
};
export type ScheduleSnapshot = { revision: number; state: AcquisitionSchedule };
export type SchedulerPolicy = Scope & {
  purpose: string;
  managerIds: readonly string[];
  workers: readonly { id: string; kind: "service" | "agent" }[];
  maximumConcurrent: number;
  leaseMs: number;
  receiptTimeoutMs?: number;
};
export type OutcomeCheck = Scope & {
  collectionId: string;
  attemptId: string;
  itemId: string;
  sourcePolicyId: string;
  requestedUrl: string;
  outcome: "captured" | "no-capture";
  sourceSha256: string | null;
  receipt: Ref;
  purpose: string;
  acquisitionKey: string;
  capture: (Omit<Capture, "bytes"> & { byteLength: number }) | null;
  failureCode: string | null;
};
export type OutcomeVerifier = {
  verify(check: OutcomeCheck, signal: AbortSignal): Promise<boolean>;
};
export type AcquisitionOutcome =
  | { capture: Capture; receipt: Ref }
  | { failureCode: string; receipt: Ref };
export type AcquisitionWork = Scope & {
  collectionId: string;
  attemptId: string;
  leaseToken: string;
  url: string;
  sourcePolicy: SourcePolicy;
  purpose: string;
  acquisitionKey: string;
  revision: number;
};
const kind = "discovery-schedule-v1";
const key = (id: string) => {
  nonempty(id, "collection identity", 128);
  return digest({ collectionId: id });
};
const json = (value: unknown): Json => JSON.parse(canonical(value)) as Json;

/** Dedicated discovery DB only. The Evidence SDK supplies generic persistence, not evidence authority. */
export class AcquisitionScheduler {
  readonly #db: Database;
  readonly #policy: SchedulerPolicy;
  readonly #verify: OutcomeVerifier["verify"];
  constructor(
    db: Database,
    policy: SchedulerPolicy,
    verifier: OutcomeVerifier,
  ) {
    this.#db = db;
    this.#policy = structuredClone(policy);
    this.#verify = verifier.verify.bind(verifier);
    nonempty(policy.purpose, "scheduler purpose", 200);
    invariant(
      Number.isSafeInteger(policy.maximumConcurrent) &&
        policy.maximumConcurrent >= 1 &&
        policy.maximumConcurrent <= 100 &&
        Number.isSafeInteger(policy.leaseMs) &&
        policy.leaseMs >= 1000 &&
        policy.leaseMs <= 600000 &&
        (policy.receiptTimeoutMs === undefined ||
          (Number.isSafeInteger(policy.receiptTimeoutMs) &&
            policy.receiptTimeoutMs >= 10 &&
            policy.receiptTimeoutMs <= 30000)),
      "invalid_scheduler",
      "Unsupported scheduler resource limits",
    );
  }
  #manager(c: Context): boolean {
    return (
      c.actor.kind === "human" &&
      c.actor.roles.includes("owner") &&
      this.#policy.managerIds.includes(c.actor.id)
    );
  }
  #guard(c: Context, mode: "manager" | "worker" | "either" = "either"): void {
    requireScope(c, this.#policy);
    validTime(c.now);
    invariant(
      c.purpose === this.#policy.purpose,
      "purpose_denied",
      "Scheduler purpose differs",
    );
    const worker =
      c.actor.roles.includes("discovery:acquire") &&
      this.#policy.workers.some(
        (w) => w.id === c.actor.id && w.kind === c.actor.kind,
      );
    invariant(
      mode === "manager"
        ? this.#manager(c)
        : mode === "worker"
          ? worker
          : this.#manager(c) || worker,
      "scheduler_denied",
      "Scheduler actor is not authorized",
    );
  }
  #command(c: Context, value: unknown): Json {
    return json({ actorKind: c.actor.kind, purpose: c.purpose, input: value });
  }
  async #read(
    tx: UnitOfWork,
    c: Context,
    id: string,
  ): Promise<ScheduleSnapshot> {
    const row = await tx.get<AcquisitionSchedule>(kind, key(id));
    invariant(row, "not_found", "Acquisition schedule not found");
    invariant(
      row.value.schemaVersion === 1 && row.value.collection.id === id,
      "invalid_snapshot",
      "Schedule identity differs",
    );
    requireScope(c, row.value.collection);
    return { revision: row.revision, state: row.value };
  }
  async #write(
    tx: UnitOfWork,
    prior: ScheduleSnapshot,
    state: AcquisitionSchedule,
    event: string,
  ): Promise<ScheduleSnapshot> {
    const id = state.collection.id,
      row = await tx.put(kind, key(id), prior.revision, state);
    await tx.enqueue(
      "discovery.schedule.changed",
      json({ collectionId: id, revision: row.revision, event }),
      digest({ kind, id, revision: row.revision }),
    );
    return { revision: row.revision, state };
  }
  #expected(snapshot: ScheduleSnapshot, revision: number): void {
    invariant(
      snapshot.revision === revision,
      "revision_conflict",
      "Acquisition schedule changed",
    );
  }
  #lease(c: Context, s: AcquisitionSchedule, token: string): AcquisitionLease {
    const lease = s.leases.find((l) => l.token === token);
    invariant(lease, "lease_not_found", "Acquisition lease not found");
    invariant(
      this.#manager(c) ||
        (lease.actorId === c.actor.id && lease.actorKind === c.actor.kind),
      "lease_owner_denied",
      "Lease belongs to another actor",
    );
    invariant(
      validTime(c.now) >= validTime(lease.leasedAt),
      "invalid_time",
      "Command precedes lease",
    );
    return lease;
  }
  async create(
    context: Context,
    input: Parameters<typeof createCollection>[1],
    commandId: string,
  ): Promise<ScheduleSnapshot> {
    const c = structuredClone(context),
      captured = structuredClone(input);
    this.#guard(c, "manager");
    return scoped(this.#db, c, (tx) =>
      tx.idempotent(
        commandId,
        this.#command(c, { type: "create", input: captured }),
        async () => {
          const collection = createCollection(c, captured),
            state: AcquisitionSchedule = {
              schemaVersion: 1,
              enabled: true,
              collection,
              leases: [],
            };
          const row = await tx.put(kind, key(collection.id), 0, state);
          return json({ revision: row.revision, state });
        },
      ),
    ) as Promise<unknown> as Promise<ScheduleSnapshot>;
  }
  async load(context: Context, id: string): Promise<ScheduleSnapshot> {
    const c = structuredClone(context);
    this.#guard(c);
    return scoped(this.#db, c, (tx) => this.#read(tx, c, id));
  }
  async leaseNext(
    context: Context,
    id: string,
    expectedRevision: number,
    commandId: string,
  ): Promise<{ snapshot: ScheduleSnapshot; lease: AcquisitionLease }> {
    return this.#leaseNext(context, id, expectedRevision, commandId);
  }
  /** Fresh lease timing; historical command replay is never execution authority. */
  async leaseNextCurrent(
    context: Context,
    id: string,
    expectedRevision: number,
    commandId: string,
    clock: () => string,
  ): Promise<{ snapshot: ScheduleSnapshot; lease: AcquisitionLease }> {
    invariant(
      typeof clock === "function",
      "invalid_clock",
      "Trusted live clock required",
    );
    return this.#leaseNext(context, id, expectedRevision, commandId, clock);
  }
  async #leaseNext(
    context: Context,
    id: string,
    expectedRevision: number,
    commandId: string,
    clock?: () => string,
  ): Promise<{ snapshot: ScheduleSnapshot; lease: AcquisitionLease }> {
    let c = structuredClone(context);
    this.#guard(c, "worker");
    return scoped(this.#db, c, (tx) =>
      tx.idempotent(
        commandId,
        this.#command(c, { type: "lease", id, expectedRevision }),
        async () => {
          const old = await this.#read(tx, c, id);
          if (clock) {
            let now: string;
            try {
              now = clock();
              nonempty(now, "Live time", 64);
              invariant(
                validTime(now) >= validTime(c.now),
                "invalid_time",
                "Live clock regressed",
              );
            } catch {
              throw new DomainError(
                "clock_unavailable",
                "Current time could not be established",
              );
            }
            c = { ...c, now };
          }
          this.#expected(old, expectedRevision);
          invariant(
            old.state.enabled,
            "scheduler_paused",
            "Acquisition schedule is paused",
          );
          invariant(
            old.state.leases.filter((l) => l.status !== "completed").length <
              this.#policy.maximumConcurrent,
            "concurrency_limit",
            "Outstanding work requires completion or reconciliation",
          );
          invariant(
            old.state.collection.attempts.length < 10000,
            "history_limit",
            "Archive this schedule before adding more attempts",
          );
          const item = dueItems(c, old.state.collection)[0];
          invariant(item, "nothing_due", "No collection item is due");
          const lease: AcquisitionLease = {
            token: newId(),
            attemptId: newId(),
            itemId: item.id,
            actorId: c.actor.id,
            actorKind: c.actor.kind,
            leasedAt: c.now,
            expiresAt: new Date(
              validTime(c.now) + this.#policy.leaseMs,
            ).toISOString(),
            receipt: null,
            status: "leased",
          };
          const state = {
            ...old.state,
            collection: startAcquisition(
              c,
              old.state.collection,
              item.id,
              lease.attemptId,
            ),
            leases: [...old.state.leases, lease],
          };
          const snapshot = await this.#write(tx, old, state, "leased");
          return json({ snapshot, lease });
        },
      ),
    ) as Promise<unknown> as Promise<{
      snapshot: ScheduleSnapshot;
      lease: AcquisitionLease;
    }>;
  }
  /** No idempotent permit replay. Commit running BEFORE returning a work description. */
  async consume(
    context: Context,
    id: string,
    expectedRevision: number,
    token: string,
  ): Promise<AcquisitionWork> {
    return this.#consume(context, id, expectedRevision, token);
  }
  /** Opt-in live admission. The trusted clock is sampled after the project lock and stored read. */
  async consumeCurrent(
    context: Context,
    id: string,
    expectedRevision: number,
    token: string,
    clock: () => string,
  ): Promise<AcquisitionWork> {
    invariant(
      typeof clock === "function",
      "invalid_clock",
      "Trusted live clock required",
    );
    return this.#consume(context, id, expectedRevision, token, clock);
  }
  async #consume(
    context: Context,
    id: string,
    expectedRevision: number,
    token: string,
    clock?: () => string,
  ): Promise<AcquisitionWork> {
    let c = structuredClone(context);
    this.#guard(c, "worker");
    return scoped(this.#db, c, async (tx) => {
      // get() alone does not lock. Hold the owning project lock BEFORE loading admission state.
      await tx.audit("discovery.schedule.consume.checked", {
        collectionId: id,
      });
      const old = await this.#read(tx, c, id);
      if (clock) {
        let now: string;
        try {
          now = clock();
          nonempty(now, "Live time", 64);
          invariant(
            validTime(now) >= validTime(c.now),
            "invalid_time",
            "Live clock regressed",
          );
        } catch {
          throw new DomainError(
            "clock_unavailable",
            "Current time could not be established",
          );
        }
        c = { ...c, now };
      }
      this.#expected(old, expectedRevision);
      const lease = this.#lease(c, old.state, token);
      invariant(
        old.state.enabled,
        "scheduler_paused",
        "Acquisition schedule is paused",
      );
      const sourcePolicy = old.state.collection.sourcePolicy;
      invariant(
        sourcePolicy.enabled === true &&
          validTime(c.now) < validTime(sourcePolicy.expiresAt),
        "connector_disabled",
        "Source policy is disabled or expired",
      );
      invariant(
        lease.status === "leased" &&
          validTime(c.now) < validTime(lease.expiresAt),
        "lease_consumed",
        "Lease expired or was already consumed",
      );
      const state = {
        ...old.state,
        leases: old.state.leases.map((l) =>
          l.token === token ? { ...l, status: "running" as const } : l,
        ),
      };
      const snapshot = await this.#write(tx, old, state, "consumed");
      const item = state.collection.items.find((i) => i.id === lease.itemId)!;
      return {
        tenantId: c.tenantId,
        projectId: c.projectId,
        collectionId: id,
        attemptId: lease.attemptId,
        leaseToken: token,
        url: item.url,
        sourcePolicy: structuredClone(sourcePolicy),
        purpose: c.purpose,
        acquisitionKey: acquisitionKey(
          state.collection,
          item.id,
          lease.attemptId,
        ),
        revision: snapshot.revision,
      };
    });
  }
  async #verified(check: OutcomeCheck): Promise<boolean> {
    const controller = new AbortController(),
      timeout = this.#policy.receiptTimeoutMs ?? 5000,
      deadline = performance.now() + timeout;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const gate = () => {
      invariant(
        !controller.signal.aborted && performance.now() < deadline,
        "receipt_timeout",
        "Outcome verification timed out",
      );
    };
    try {
      return await Promise.race([
        Promise.resolve()
          .then(() => {
            gate();
            const captured = structuredClone(check);
            gate();
            return this.#verify(captured, controller.signal);
          })
          .then(
            (value) => {
              gate();
              return value === true;
            },
            () => {
              gate();
              throw new DomainError(
                "receipt_unavailable",
                "Outcome verifier could not complete",
              );
            },
          ),
        new Promise<boolean>((_, reject) => {
          timer = setTimeout(() => {
            controller.abort();
            reject(
              new DomainError(
                "receipt_timeout",
                "Outcome verification timed out",
              ),
            );
          }, timeout);
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
      controller.abort();
    }
  }

  async complete(
    context: Context,
    id: string,
    expectedRevision: number,
    token: string,
    outcome: AcquisitionOutcome,
    commandId: string,
  ): Promise<ScheduleSnapshot> {
    const c = structuredClone(context),
      result = structuredClone(outcome);
    this.#guard(c);
    validateRef(result.receipt);
    requireScope(c, result.receipt);
    const capture = "capture" in result ? result.capture : null;
    if (capture)
      invariant(
        capture.status === 200 &&
          [
            "text/plain",
            "text/html",
            "application/json",
            "application/pdf",
          ].includes(capture.contentType) &&
          capture.bytes.length <= 25 * 1024 * 1024 &&
          hashBytes(capture.bytes) === capture.sha256,
        "integrity_failure",
        "Outcome bytes differ",
      );
    // Fingerprint exact metadata and digest, not a JSON expansion of binary bytes.
    const descriptor = capture
      ? (({ bytes, ...metadata }) => ({
          ...metadata,
          byteLength: bytes.length,
        }))(capture)
      : null;
    const fingerprint = descriptor ?? result;
    return scoped(this.#db, c, (tx) =>
      tx.idempotent(
        commandId,
        this.#command(c, {
          type: "complete",
          id,
          expectedRevision,
          token,
          outcome: fingerprint,
          receipt: result.receipt,
        }),
        async () => {
          const old = await this.#read(tx, c, id);
          this.#expected(old, expectedRevision);
          const lease = this.#lease(c, old.state, token);
          invariant(
            lease.status === "running" || lease.status === "reconcile",
            "lease_not_running",
            "Only consumed work can report an outcome",
          );
          const collection = finishAcquisition(
            c,
            old.state.collection,
            lease.attemptId,
            capture
              ? { capture, receiptId: result.receipt.id }
              : {
                  failureCode: (result as { failureCode: string }).failureCode,
                },
          );
          const item = collection.items.find((i) => i.id === lease.itemId)!;
          invariant(
            await this.#verified({
              tenantId: c.tenantId,
              projectId: c.projectId,
              collectionId: id,
              attemptId: lease.attemptId,
              itemId: item.id,
              sourcePolicyId: collection.sourcePolicy.id,
              requestedUrl: item.url,
              outcome: capture ? "captured" : "no-capture",
              sourceSha256: capture?.sha256 ?? null,
              receipt: result.receipt,
              purpose: c.purpose,
              acquisitionKey: acquisitionKey(
                collection,
                item.id,
                lease.attemptId,
              ),
              capture: descriptor,
              failureCode: "failureCode" in result ? result.failureCode : null,
            }),
            "receipt_denied",
            "Outcome receipt does not verify this attempt",
          );
          const state = {
            ...old.state,
            collection,
            leases: old.state.leases.map((l) =>
              l.token === token
                ? {
                    ...l,
                    status: "completed" as const,
                    receipt: result.receipt,
                  }
                : l,
            ),
          };
          return json(await this.#write(tx, old, state, "completed"));
        },
      ),
    ) as Promise<unknown> as Promise<ScheduleSnapshot>;
  }
  /** Expired unconsumed leases are safe failures. Consumed work stays blocked until verified reconciliation. */
  async recover(
    context: Context,
    id: string,
    expectedRevision: number,
    commandId: string,
  ): Promise<ScheduleSnapshot> {
    return this.#recover(context, id, expectedRevision, commandId);
  }
  /** Current expiry observation only: consumed work never becomes retryable. */
  async recoverCurrent(
    context: Context,
    id: string,
    expectedRevision: number,
    commandId: string,
    clock: () => string,
  ): Promise<ScheduleSnapshot> {
    invariant(
      typeof clock === "function",
      "invalid_clock",
      "Trusted live clock required",
    );
    return this.#recover(context, id, expectedRevision, commandId, clock);
  }
  async #recover(
    context: Context,
    id: string,
    expectedRevision: number,
    commandId: string,
    clock?: () => string,
  ): Promise<ScheduleSnapshot> {
    let c = structuredClone(context);
    this.#guard(c);
    return scoped(this.#db, c, (tx) =>
      tx.idempotent(
        commandId,
        this.#command(c, { type: "recover", id, expectedRevision }),
        async () => {
          const old = await this.#read(tx, c, id);
          if (clock) {
            let now: string;
            try {
              now = clock();
              nonempty(now, "Live time", 64);
              invariant(
                validTime(now) >= validTime(c.now),
                "invalid_time",
                "Live clock regressed",
              );
            } catch {
              throw new DomainError(
                "clock_unavailable",
                "Current time could not be established",
              );
            }
            c = { ...c, now };
          }
          this.#expected(old, expectedRevision);
          let collection = old.state.collection,
            changed = false;
          const leases = old.state.leases.map((lease) => {
            if (
              !["leased", "running"].includes(lease.status) ||
              validTime(c.now) < validTime(lease.expiresAt)
            )
              return lease;
            changed = true;
            if (lease.status === "leased") {
              collection = finishAcquisition(c, collection, lease.attemptId, {
                failureCode: "lease_expired_unconsumed",
              });
              return { ...lease, status: "completed" as const };
            }
            return { ...lease, status: "reconcile" as const };
          });
          if (!changed) return json(old);
          return json(
            await this.#write(
              tx,
              old,
              { ...old.state, collection, leases },
              "recovered",
            ),
          );
        },
      ),
    ) as Promise<unknown> as Promise<ScheduleSnapshot>;
  }
  async setEnabled(
    context: Context,
    id: string,
    expectedRevision: number,
    enabled: boolean,
    commandId: string,
  ): Promise<ScheduleSnapshot> {
    const c = structuredClone(context);
    this.#guard(c, "manager");
    invariant(
      typeof enabled === "boolean",
      "invalid_input",
      "Boolean enabled state required",
    );
    return scoped(this.#db, c, (tx) =>
      tx.idempotent(
        commandId,
        this.#command(c, { type: "enabled", id, expectedRevision, enabled }),
        async () => {
          const old = await this.#read(tx, c, id);
          this.#expected(old, expectedRevision);
          if (old.state.enabled === enabled) return json(old);
          return json(
            await this.#write(
              tx,
              old,
              { ...old.state, enabled },
              enabled ? "enabled" : "paused",
            ),
          );
        },
      ),
    ) as Promise<unknown> as Promise<ScheduleSnapshot>;
  }
}
