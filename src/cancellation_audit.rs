/// Backend audit trail for subscription cancellations.
///
/// Persists off-chain cancellation events with timestamps, subscription
/// metadata, and an optional reason so merchants have a queryable history.
///
/// Per-contributor history is bounded to [`MAX_AUDIT_HISTORY_PER_USER`] entries
/// using ring-buffer semantics: the oldest record is evicted when the cap is
/// reached, keeping persistent storage rent constant per active contributor.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Maximum number of cancellation audit records retained per contributor.
///
/// When a new record would exceed this ceiling the oldest existing record for
/// that contributor is removed before the new one is inserted, maintaining
/// ring-buffer semantics and a constant storage footprint.
pub const MAX_AUDIT_HISTORY_PER_USER: u32 = 50;

/// The reason a subscription was cancelled.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancellationReason {
    MerchantRevoked,
    ContributorWithdrew,
    AdminOverride,
    Other(String),
}

/// A single cancellation audit record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancellationRecord {
    /// Unique audit record ID (UUID v4).
    pub id: String,
    /// On-chain org identifier.
    pub org_id: String,
    /// On-chain issue / subscription identifier.
    pub issue_id: u64,
    /// Contributor address that held the subscription.
    pub contributor: String,
    /// Who triggered the cancellation.
    pub cancelled_by: String,
    /// UTC timestamp of the cancellation.
    pub cancelled_at: DateTime<Utc>,
    /// Optional human-readable reason.
    pub reason: Option<CancellationReason>,
    /// Stellar transaction hash for the on-chain event (if available).
    pub tx_hash: Option<String>,
}

/// Persists a cancellation record to the audit store.
///
/// In production this writes to the backing database (Postgres / DynamoDB).
/// The trait keeps the service testable without a live DB.
pub trait AuditStore: Send + Sync {
    fn insert(&self, record: &CancellationRecord) -> Result<(), AuditError>;
    fn list_by_org(&self, org_id: &str) -> Result<Vec<CancellationRecord>, AuditError>;
    fn list_by_contributor(
        &self,
        contributor: &str,
    ) -> Result<Vec<CancellationRecord>, AuditError>;
    /// Return the number of audit records stored for a specific contributor.
    fn count_by_contributor(&self, contributor: &str) -> Result<u32, AuditError>;
    /// Remove the oldest audit record for a contributor (ring-buffer eviction).
    ///
    /// "Oldest" is determined by `cancelled_at`. When multiple records share
    /// the same timestamp the implementation may remove any one of them.
    fn remove_oldest_for_contributor(&self, contributor: &str) -> Result<(), AuditError>;
}

#[derive(Debug)]
pub enum AuditError {
    StorageFailure(String),
    RecordNotFound,
}

impl std::fmt::Display for AuditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuditError::StorageFailure(msg) => write!(f, "storage failure: {msg}"),
            AuditError::RecordNotFound => write!(f, "record not found"),
        }
    }
}

/// High-level service used by API handlers.
pub struct CancellationAuditService<S: AuditStore> {
    store: S,
}

impl<S: AuditStore> CancellationAuditService<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// Record a new cancellation event.
    ///
    /// Before inserting, the per-contributor history is checked against
    /// [`MAX_AUDIT_HISTORY_PER_USER`]. If the cap is reached, the oldest
    /// record for the contributor is evicted to maintain ring-buffer semantics
    /// and keep the persistent storage footprint constant.
    pub fn record(
        &self,
        org_id: impl Into<String>,
        issue_id: u64,
        contributor: impl Into<String>,
        cancelled_by: impl Into<String>,
        reason: Option<CancellationReason>,
        tx_hash: Option<String>,
    ) -> Result<CancellationRecord, AuditError> {
        let record = CancellationRecord {
            id: uuid::Uuid::new_v4().to_string(),
            org_id: org_id.into(),
            issue_id,
            contributor: contributor.into(),
            cancelled_by: cancelled_by.into(),
            cancelled_at: Utc::now(),
            reason,
            tx_hash,
        };

        // Enforce per-contributor history cap (ring-buffer semantics).
        let count = self.store.count_by_contributor(&record.contributor)?;
        if count >= MAX_AUDIT_HISTORY_PER_USER {
            self.store.remove_oldest_for_contributor(&record.contributor)?;
        }

        self.store.insert(&record)?;
        Ok(record)
    }

    pub fn history_for_org(
        &self,
        org_id: &str,
    ) -> Result<Vec<CancellationRecord>, AuditError> {
        self.store.list_by_org(org_id)
    }

    pub fn history_for_contributor(
        &self,
        contributor: &str,
    ) -> Result<Vec<CancellationRecord>, AuditError> {
        self.store.list_by_contributor(contributor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MemStore(Mutex<Vec<CancellationRecord>>);

    impl AuditStore for MemStore {
        fn insert(&self, record: &CancellationRecord) -> Result<(), AuditError> {
            self.0.lock().unwrap().push(record.clone());
            Ok(())
        }

        fn list_by_org(&self, org_id: &str) -> Result<Vec<CancellationRecord>, AuditError> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.org_id == org_id)
                .cloned()
                .collect())
        }

        fn list_by_contributor(
            &self,
            contributor: &str,
        ) -> Result<Vec<CancellationRecord>, AuditError> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.contributor == contributor)
                .cloned()
                .collect())
        }

        fn count_by_contributor(&self, contributor: &str) -> Result<u32, AuditError> {
            let count = self
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.contributor == contributor)
                .count();
            Ok(count as u32)
        }

        fn remove_oldest_for_contributor(&self, contributor: &str) -> Result<(), AuditError> {
            let mut store = self.0.lock().unwrap();
            // Find the index of the entry with the earliest `cancelled_at` for
            // this contributor.
            let oldest_idx = store
                .iter()
                .enumerate()
                .filter(|(_, r)| r.contributor == contributor)
                .min_by_key(|(_, r)| r.cancelled_at)
                .map(|(i, _)| i);

            match oldest_idx {
                Some(idx) => {
                    store.remove(idx);
                    Ok(())
                }
                None => Err(AuditError::RecordNotFound),
            }
        }
    }

    fn svc() -> CancellationAuditService<MemStore> {
        CancellationAuditService::new(MemStore(Mutex::new(vec![])))
    }

    #[test]
    fn record_stores_event() {
        let s = svc();
        let rec = s
            .record("org-1", 42, "GZZZ", "GXXX", Some(CancellationReason::MerchantRevoked), None)
            .unwrap();
        assert_eq!(rec.org_id, "org-1");
        assert_eq!(rec.issue_id, 42);
        assert!(rec.reason.is_some());
    }

    #[test]
    fn history_for_org_filters_correctly() {
        let s = svc();
        s.record("org-1", 1, "G1", "G2", None, None).unwrap();
        s.record("org-2", 2, "G3", "G4", None, None).unwrap();
        assert_eq!(s.history_for_org("org-1").unwrap().len(), 1);
        assert_eq!(s.history_for_org("org-2").unwrap().len(), 1);
    }

    #[test]
    fn history_for_contributor_filters_correctly() {
        let s = svc();
        s.record("org-1", 1, "G1", "G2", None, None).unwrap();
        s.record("org-1", 2, "G1", "G2", None, None).unwrap();
        s.record("org-1", 3, "G9", "G2", None, None).unwrap();
        assert_eq!(s.history_for_contributor("G1").unwrap().len(), 2);
    }

    /// Inserting more than MAX_AUDIT_HISTORY_PER_USER records for a single
    /// contributor must not grow the stored history beyond the cap.
    ///
    /// After 100 consecutive cancellations the contributor's history must
    /// contain exactly MAX_AUDIT_HISTORY_PER_USER (50) entries — the oldest
    /// are evicted in ring-buffer fashion to keep storage rent constant.
    #[test]
    fn bounded_history_stays_at_50() {
        let s = svc();
        // Insert 100 records for the same contributor
        for i in 0..100u64 {
            s.record("org-1", i, "G1", "G2", None, None).unwrap();
        }
        // History must be capped at MAX_AUDIT_HISTORY_PER_USER
        let history = s.history_for_contributor("G1").unwrap();
        assert_eq!(history.len(), MAX_AUDIT_HISTORY_PER_USER as usize);
    }
}
