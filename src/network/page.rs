//! Shared byte accounting for remote pages, before cloning source records.

use crate::error::{BrokerError, ErrorCode};

pub(crate) struct PageBudget {
    remaining: usize,
    items: usize,
    limit: usize,
}

impl PageBudget {
    pub(crate) fn new(limit: usize, bytes: usize) -> Self {
        Self {
            remaining: bytes,
            items: 0,
            limit,
        }
    }

    /// The caller passes a borrowed source view. A full page leaves the next
    /// item untouched; an oversized first item fails without cursor progress.
    pub(crate) fn admit<T: serde::Serialize>(&mut self, item: &T) -> Result<bool, BrokerError> {
        if self.items == self.limit {
            return Ok(false);
        }
        let size = crate::rpc::bounded_json_size(item, self.remaining.saturating_sub(1));
        let Some(size) = size else {
            if self.items == 0 {
                return Err(BrokerError::application(
                    ErrorCode::ResourceExhausted,
                    "next item exceeds remote page budget",
                ));
            }
            return Ok(false);
        };
        self.remaining -= size + 1;
        self.items += 1;
        Ok(true)
    }
}
