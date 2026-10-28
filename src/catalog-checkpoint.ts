import { scoped } from "@accountability/evidence/database";
import type { Database, UnitOfWork } from "@accountability/evidence/database";
import { digest, invariant, nonempty, requireScope } from "./contracts.ts";
import type { Context } from "./contracts.ts";
import { CatalogTraversal } from "./catalog-traversal.ts";
import type {
  CatalogTraversalLimits,
  CatalogTraversalSnapshot,
} from "./catalog-traversal.ts";
import type { JsonArchiveConfiguration } from "./connectors.ts";
import type { Capture } from "./index.ts";

export type CatalogCheckpoint = {
  schemaVersion: 1;
  id: string;
  purpose: string;
  configurationDigest: string;
  limitsDigest: string;
  traversal: CatalogTraversalSnapshot;
};

export type CatalogCheckpointSnapshot = {
  revision: number;
  state: CatalogCheckpoint;
};

const kind = "catalog-traversal-v1";
const key = (id: string) => {
  nonempty(id, "catalog checkpoint identity", 128);
  return digest({ checkpointId: id });
};

/**
 * Opt-in operational metadata persistence. Captured bytes and evidence receipts
 * remain owned by the trusted host and evidence quarantine.
 */
export class CatalogCheckpointStore {
  readonly #db: Database;
  readonly #configuration: JsonArchiveConfiguration;
  readonly #limits: CatalogTraversalLimits;
  readonly #configurationDigest: string;
  readonly #limitsDigest: string;

  constructor(
    database: Database,
    configuration: JsonArchiveConfiguration,
    limits: CatalogTraversalLimits,
  ) {
    this.#db = database;
    this.#configuration = structuredClone(configuration);
    this.#limits = structuredClone(limits);
    this.#configurationDigest = digest(this.#configuration);
    this.#limitsDigest = digest(this.#limits);
  }

  #guard(context: Context): void {
    requireScope(context, this.#configuration.catalogPolicy);
    requireScope(context, this.#configuration.itemPolicy);
    nonempty(context.purpose, "catalog purpose", 200);
  }

  async #read(
    tx: UnitOfWork,
    context: Context,
    id: string,
  ): Promise<CatalogCheckpointSnapshot> {
    const row = await tx.get<CatalogCheckpoint>(kind, key(id));
    invariant(row, "not_found", "Catalog checkpoint not found");
    const state = row.value;
    invariant(
      state.schemaVersion === 1 && state.id === id,
      "invalid_checkpoint",
      "Catalog checkpoint identity differs",
    );
    requireScope(context, state.traversal);
    invariant(
      state.purpose === context.purpose,
      "purpose_denied",
      "Traversal purpose differs",
    );
    invariant(
      state.configurationDigest === this.#configurationDigest &&
        state.limitsDigest === this.#limitsDigest,
      "checkpoint_binding_mismatch",
      "Checkpoint connector configuration or limits differ",
    );
    return { revision: row.revision, state };
  }

  async create(
    context: Context,
    id: string,
    rootUrl: string,
  ): Promise<CatalogCheckpointSnapshot> {
    const c = structuredClone(context);
    this.#guard(c);
    nonempty(id, "catalog checkpoint identity", 128);
    const traversal = new CatalogTraversal(
      c,
      this.#configuration,
      rootUrl,
      this.#limits,
    );
    const state: CatalogCheckpoint = {
      schemaVersion: 1,
      id,
      purpose: c.purpose,
      configurationDigest: this.#configurationDigest,
      limitsDigest: this.#limitsDigest,
      traversal: traversal.snapshot(),
    };
    return scoped(this.#db, c, async (tx) => {
      const row = await tx.put(kind, key(id), 0, state);
      await tx.enqueue(
        "catalog.traversal.changed",
        { checkpointId: id, revision: row.revision, event: "created" },
        digest({ kind, id, revision: row.revision }),
      );
      return { revision: row.revision, state: structuredClone(state) };
    });
  }

  async load(
    context: Context,
    id: string,
  ): Promise<CatalogCheckpointSnapshot> {
    const c = structuredClone(context);
    this.#guard(c);
    return scoped(this.#db, c, (tx) => this.#read(tx, c, id));
  }

  async append(
    context: Context,
    id: string,
    expectedRevision: number,
    source: Capture,
  ): Promise<CatalogCheckpointSnapshot> {
    const c = structuredClone(context),
      captured = structuredClone(source);
    this.#guard(c);
    return scoped(this.#db, c, async (tx) => {
      await tx.audit("catalog.traversal.append.checked", {
        checkpointId: id,
      });
      const prior = await this.#read(tx, c, id);
      invariant(
        prior.revision === expectedRevision,
        "revision_conflict",
        "Catalog checkpoint changed",
      );
      const traversal = CatalogTraversal.resume(
        c,
        this.#configuration,
        this.#limits,
        prior.state.purpose,
        prior.state.traversal,
      );
      const state: CatalogCheckpoint = {
        ...prior.state,
        traversal: traversal.append(c, captured),
      };
      const row = await tx.put(kind, key(id), prior.revision, state);
      await tx.enqueue(
        "catalog.traversal.changed",
        { checkpointId: id, revision: row.revision, event: "page-appended" },
        digest({ kind, id, revision: row.revision }),
      );
      return { revision: row.revision, state: structuredClone(state) };
    });
  }
}
