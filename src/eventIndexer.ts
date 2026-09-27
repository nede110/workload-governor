/**
 * eventIndexer.ts
 *
 * Polls the Soroban RPC node every 5 seconds for WorkloadGovernor contract events,
 * parses them into typed DB records, and persists them with deduplication.
 *
 * Supported event types (matching src/events.rs emit helpers):
 *   applied, withdrew, assigned, completed, revoked, maintainer_registered
 *
 * Deduplication key: (ledger_sequence, transaction_hash, event_index) —
 *   INSERT … ON CONFLICT DO NOTHING prevents duplicate rows even after a
 *   full-history replay caused by cursor loss (fixes issue #575).
 *
 * Resume: on startup, reads the highest ledger already stored and continues
 *   from there, relying on the unique constraint to skip already-seen events.
 */

import { SorobanRpc, xdr as stellarXdr, scValToNative } from '@stellar/stellar-sdk';
import { pool } from './db';
import { logger } from './logger';
import { publishLiveEvent } from './services/event-bus';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const CONTRACT_ID =
  process.env['CONTRACT_ID'] ??
  'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4';

const RPC_URL =
  process.env['SOROBAN_RPC_URL'] ?? 'https://soroban-testnet.stellar.org';

const POLL_INTERVAL_MS = 5_000;
const ERROR_BACKOFF_MS = 10_000;

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

export type ContractEventType =
  | 'applied'
  | 'withdrew'
  | 'assigned'
  | 'completed'
  | 'revoked'
  | 'maintainer_registered';

/**
 * Normalized DB record for a single contract event.
 * Stored in the `contract_events` table.
 */
export interface ContractEventRecord {
  event_type: ContractEventType;
  contributor: string | null;
  org_id: string | null;
  issue_id: number | null;
  tx_hash: string;
  event_index: number;
  ledger: number;
  timestamp: Date;
}

// ---------------------------------------------------------------------------
// XDR helpers
// ---------------------------------------------------------------------------

/**
 * Safely decode an XDR base64 string to its native JS value.
 * Returns null if decoding fails.
 */
function decodeScVal(xdrBase64: string): unknown {
  try {
    const scVal = stellarXdr.ScVal.fromXDR(xdrBase64, 'base64');
    return scValToNative(scVal);
  } catch {
    return null;
  }
}

/**
 * Extract the symbol string from a Soroban ScVal topic (first topic slot).
 * The event type is emitted as a Symbol in slot[0].
 */
function extractEventType(topics: string[]): ContractEventType | null {
  if (topics.length === 0) return null;
  const val = decodeScVal(topics[0]);
  if (typeof val !== 'string') return null;
  const known: ContractEventType[] = [
    'applied',
    'withdrew',
    'assigned',
    'completed',
    'revoked',
    'maintainer_registered',
  ];
  return known.includes(val as ContractEventType) ? (val as ContractEventType) : null;
}

/**
 * Extract the contributor address from a Soroban ScVal topic (second topic slot).
 * For all 5 state-change events the contributor is in topics[1].
 */
function extractContributorFromTopic(topics: string[]): string | null {
  if (topics.length < 2) return null;
  const val = decodeScVal(topics[1]);
  if (typeof val === 'string') return val;
  return null;
}

/**
 * Parse the data value tuple emitted with each event.
 *
 * Event data layouts (from src/events.rs):
 *   applied    → data = (org_id: Symbol, issue_id: u32)
 *   withdrew   → data = (org_id: Symbol, issue_id: u32)
 *   assigned   → data = (maintainer: Address, org_id: Symbol, issue_id: u32)
 *   completed  → data = (maintainer: Address, org_id: Symbol, issue_id: u32)
 *   revoked    → data = (maintainer: Address, org_id: Symbol, issue_id: u32)
 *   maintainer_registered → data = org_id: Symbol (scalar, not tuple)
 */
interface ParsedData {
  org_id: string | null;
  issue_id: number | null;
}

function parseEventData(dataXdr: string, eventType: ContractEventType): ParsedData {
  const raw = decodeScVal(dataXdr);

  if (eventType === 'maintainer_registered') {
    // data is a plain Symbol
    return {
      org_id: typeof raw === 'string' ? raw : null,
      issue_id: null,
    };
  }

  // All other events emit a tuple
  if (!Array.isArray(raw)) {
    return { org_id: null, issue_id: null };
  }

  if (eventType === 'applied' || eventType === 'withdrew') {
    // (org_id, issue_id)
    const [orgId, issueId] = raw as [unknown, unknown];
    return {
      org_id: typeof orgId === 'string' ? orgId : null,
      issue_id: typeof issueId === 'number' ? issueId : null,
    };
  }

  // assigned / completed / revoked → (maintainer, org_id, issue_id)
  const [, orgId, issueId] = raw as [unknown, unknown, unknown];
  return {
    org_id: typeof orgId === 'string' ? orgId : null,
    issue_id: typeof issueId === 'number' ? issueId : null,
  };
}

// ---------------------------------------------------------------------------
// RPC event shape (SDK v11 / RPC spec)
// ---------------------------------------------------------------------------

interface RpcEvent {
  /** "contract" | "system" | "diagnostic" */
  type: string;
  /** "<ledger>-<tx_index>-<event_index>" — paging cursor */
  id: string;
  pagingToken: string;
  /** ledger sequence number as string */
  ledger: string;
  /** ISO-8601 creation timestamp */
  createdAt: string;
  txHash?: string;
  /** Hash of this ledger — used to track the confirmed chain tip. */
  ledgerHash?: string;
  /** Hash of the parent (previous) ledger — used for reorg detection. */
  parentLedgerHash?: string;
  topic: Array<{ type: string; xdr: string }>;
  value: { type: string; xdr: string };
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/**
 * Running total of ledger reorganizations detected by the indexer.
 * Equivalent to the Prometheus counter `indexer_reorg_detected_total`.
 */
let indexerReorgDetectedTotal = 0;

/** Return the current value of the reorg detection counter. */
export function getReorgDetectedTotal(): number {
  return indexerReorgDetectedTotal;
}

// ---------------------------------------------------------------------------
// EventIndexer class
// ---------------------------------------------------------------------------

export class EventIndexer {
  private server: SorobanRpc.Server;
  /** Paging cursor for the next RPC call. Undefined means start from resume ledger. */
  private cursor: string | undefined;
  private isRunning = false;
  /** Ledger hash of the last successfully processed batch — used for reorg detection. */
  private lastConfirmedLedgerHash: string | undefined = undefined;
  /** Ledger sequence of the last successfully processed batch. */
  private lastConfirmedLedger: number = 0;

  constructor() {
    this.server = new SorobanRpc.Server(RPC_URL, { allowHttp: true });
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────

  async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;

    // Resume from the last successfully indexed ledger
    await this.initCursor();

    logger.info({ message: 'Event indexer started', contract: CONTRACT_ID, rpc: RPC_URL });

    this.pollForEvents().catch((err) => {
      logger.error({
        message: 'Event indexer fatal error',
        error: err instanceof Error ? err.message : String(err),
        stack: err instanceof Error ? err.stack : undefined,
      });
      this.isRunning = false;
    });
  }

  stop(): void {
    this.isRunning = false;
    logger.info({ message: 'Event indexer stopped' });
  }

  // ── Cursor / resume ──────────────────────────────────────────────────────

  /**
   * On restart, re-process from the last finalized ledger to handle reorgs.
   * Uses the highest ledger_seq stored in contract_events as the resume point.
   *
   * Because INSERT uses ON CONFLICT DO NOTHING, replaying events from the
   * resume ledger is safe — already-indexed events are silently skipped.
   * This prevents duplicates even if the cursor is lost (fixes issue #575).
   */
  private async initCursor(): Promise<void> {
    try {
      const { rows } = await pool.query<{ max_ledger: string | null }>(
        'SELECT MAX(ledger_seq) AS max_ledger FROM contract_events',
      );
      const maxLedger = rows[0]?.max_ledger != null ? parseInt(rows[0].max_ledger, 10) : null;

      if (maxLedger !== null && maxLedger > 0) {
        // Use the last finalized ledger as the start cursor so we re-fetch
        // that ledger's events and handle any potential reorg.
        // The cursor format expected by getEvents is "<ledger>-<tx>-<event>"
        // Passing just the ledger number as a numeric string is also accepted.
        this.cursor = String(maxLedger);
        logger.info({ message: 'Resuming indexer from ledger', ledger: maxLedger });
      } else {
        this.cursor = undefined;
        logger.info({ message: 'Starting indexer from genesis (no stored events)' });
      }
    } catch (err) {
      // Table might not exist yet; start from genesis
      logger.warn({
        message: 'Could not read resume ledger, starting from genesis',
        error: err instanceof Error ? err.message : String(err),
      });
      this.cursor = undefined;
    }
  }

  // ── Poll loop ────────────────────────────────────────────────────────────

  /**
   * Roll back events from orphaned ledgers after a chain reorganization.
   *
   * Deletes all `contract_events` rows whose `ledger_seq` is strictly greater
   * than `commonAncestorLedger` in a single atomic database transaction.
   * After a successful rollback the cursor is reset to re-index from the safe
   * ancestor ledger.
   */
  private async rollbackToLedger(commonAncestorLedger: number): Promise<void> {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const result = await client.query(
        'DELETE FROM contract_events WHERE ledger_seq > $1',
        [commonAncestorLedger],
      );
      await client.query('COMMIT');
      const deleted = (result as { rowCount?: number }).rowCount ?? 0;
      logger.warn({
        message: 'Rolled back orphaned ledger events',
        commonAncestorLedger,
        deletedRows: deleted,
      });
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  private async pollForEvents(): Promise<void> {
    while (this.isRunning) {
      try {
        const response = await this.server.getEvents({
          filters: [
            {
              type: 'contract',
              contractIds: [CONTRACT_ID],
            },
          ],
          cursor: this.cursor,
          limit: 200,
        });

        const events = response.events as unknown as RpcEvent[];

        // ── Reorg detection ───────────────────────────────────────────────
        // Compare the parentLedgerHash of the first event's ledger against
        // the last confirmed hash we have stored. A mismatch means the RPC
        // node reorganized and we must roll back orphaned events before
        // re-indexing from the safe ancestor ledger.
        if (events.length > 0 && this.lastConfirmedLedgerHash !== undefined) {
          const firstEvent = events[0];
          const firstLedger = parseInt(firstEvent.ledger, 10);
          const parentHash = firstEvent.parentLedgerHash;

          if (
            parentHash !== undefined &&
            firstLedger === this.lastConfirmedLedger + 1 &&
            parentHash !== this.lastConfirmedLedgerHash
          ) {
            // Hash mismatch — ledger reorganization detected.
            indexerReorgDetectedTotal++;
            logger.warn({
              message: 'Ledger reorganization detected',
              expectedParentHash: this.lastConfirmedLedgerHash,
              actualParentHash: parentHash,
              reorgAtLedger: firstLedger,
              metric: 'indexer_reorg_detected_total',
              metricValue: indexerReorgDetectedTotal,
            });

            // Roll back to the ledger just before the divergence point.
            await this.rollbackToLedger(this.lastConfirmedLedger - 1);

            // Reset cursor so we re-index from before the reorg.
            this.cursor = String(Math.max(0, this.lastConfirmedLedger - 1));
            this.lastConfirmedLedgerHash = undefined;
            this.lastConfirmedLedger = 0;

            // Re-poll immediately after rollback without sleeping.
            continue;
          }
        }
        // ── End reorg detection ───────────────────────────────────────────

        if (events.length > 0) {
          let persisted = 0;
          let skipped = 0;

          for (const raw of events) {
            const record = this.parseRpcEvent(raw);
            if (!record) {
              skipped++;
              continue;
            }
            const stored = await this.storeEvent(record);
            if (stored) persisted++;
            else skipped++;
          }

          // Advance cursor to the last event's pagingToken
          const last = events[events.length - 1];
          this.cursor = last.pagingToken ?? last.id;

          // Track the latest confirmed ledger hash for reorg detection on the
          // next poll cycle.
          if (last.ledgerHash !== undefined) {
            this.lastConfirmedLedgerHash = last.ledgerHash;
            this.lastConfirmedLedger = parseInt(last.ledger, 10);
          }

          logger.info({
            message: 'Indexed event batch',
            ledger: parseInt(events[events.length - 1].ledger, 10),
            total: events.length,
            persisted,
            skipped,
          });
        }

        await sleep(POLL_INTERVAL_MS);
      } catch (err) {
        logger.error({
          message: 'Event polling error',
          error: err instanceof Error ? err.message : String(err),
        });
        await sleep(ERROR_BACKOFF_MS);
      }
    }
  }

  // ── Event parsing ────────────────────────────────────────────────────────

  /**
   * Convert a raw RPC event object into a ContractEventRecord.
   * Returns null if the event is unknown or malformed.
   */
  private parseRpcEvent(raw: RpcEvent): ContractEventRecord | null {
    try {
      // Only process contract events
      if (raw.type !== 'contract') return null;

      const topics = raw.topic?.map((t) => t.xdr) ?? [];
      const dataXdr = raw.value?.xdr ?? '';

      const eventType = extractEventType(topics);
      if (!eventType) return null;

      const contributor =
        eventType === 'maintainer_registered'
          ? null
          : extractContributorFromTopic(topics);

      const { org_id, issue_id } = parseEventData(dataXdr, eventType);

      // Parse the event index from the id string: "<ledger>-<tx_index>-<event_index>"
      const idParts = raw.id.split('-');
      const eventIndex = idParts.length >= 3 ? parseInt(idParts[2], 10) : 0;

      // tx_hash may be undefined for some synthetic events; fall back to id
      const txHash = raw.txHash ?? raw.id;

      const ledger = parseInt(raw.ledger, 10);
      const timestamp = new Date(raw.createdAt);

      return {
        event_type: eventType,
        contributor,
        org_id,
        issue_id,
        tx_hash: txHash,
        event_index: eventIndex,
        ledger,
        timestamp,
      };
    } catch {
      return null;
    }
  }

  // ── Persistence ──────────────────────────────────────────────────────────

  /**
   * Insert a contract event record.
   *
   * Uses (ledger_seq, tx_hash, event_index) as the deduplication key.
   * Duplicate rows are silently skipped (ON CONFLICT DO NOTHING).
   *
   * This is safe to call after a full-history replay caused by cursor loss:
   * the unique constraint added in migration 2_event_deduplication.js
   * prevents duplicates at the database level regardless of how many times
   * the same event is re-submitted (fixes issue #575).
   *
   * @returns true if a new row was inserted, false if it was a duplicate.
   */
  private async storeEvent(record: ContractEventRecord): Promise<boolean> {
    const result = await pool.query(
      `INSERT INTO contract_events
         (event_type, contributor, org_id, issue_id, tx_hash, event_index, ledger_seq, timestamp)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (ledger_seq, tx_hash, event_index) DO NOTHING`,
      [
        record.event_type,
        record.contributor,
        record.org_id,
        record.issue_id,
        record.tx_hash,
        record.event_index,
        record.ledger,
        record.timestamp,
      ],
    );

    const inserted = (result as { rowCount?: number }).rowCount === 1;

    if (inserted) {
      // Publish live update for real-time subscribers
      const liveType =
        record.event_type === 'applied'
          ? 'application_created'
          : record.event_type === 'assigned'
            ? 'assignment_created'
            : 'cap_updated';

      publishLiveEvent({
        type: liveType,
        data: {
          eventType: record.event_type,
          orgId: record.org_id,
          issueId: record.issue_id,
        },
      });
    }

    return inserted;
  }
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Module-level singleton helpers
// ---------------------------------------------------------------------------

let _indexer: EventIndexer | null = null;

export function getEventIndexer(): EventIndexer {
  if (!_indexer) _indexer = new EventIndexer();
  return _indexer;
}

export async function startEventIndexer(): Promise<void> {
  await getEventIndexer().start();
}

export function stopEventIndexer(): void {
  _indexer?.stop();
}
