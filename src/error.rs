//! Error taxonomy shared by the daemon, CLI, and later MCP adapters.
//!
//! The symbolic names are the contract; the JSON-RPC codes are the wire
//! encoding used by the Unix-socket transport. All application errors use the
//! server-reserved range `-32099..=-32000`, keeping them distinct from the
//! standard JSON-RPC protocol errors (`-32700`, `-32600`, `-32601`, ...).
//!
//! | Name | JSON-RPC code |
//! |---|---|
//! | `UNAUTHENTICATED` | -32001 |
//! | `NOT_FOUND` | -32002 |
//! | `RESOURCE_EXHAUSTED` | -32003 |
//! | `CONFLICT` | -32004 |
//! | `EXISTS` | -32005 |
//! | `VERSION_CONFLICT` | -32006 |
//! | `CONFLICT_CLAIMED` | -32007 |
//! | `LEASE_LOST` | -32008 |
//! | `DAEMON_UNREACHABLE` | -32009 |

/// Canonical broker error codes from design §5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    Unauthenticated,
    NotFound,
    ResourceExhausted,
    Conflict,
    Exists,
    VersionConflict,
    ConflictClaimed,
    LeaseLost,
    DaemonUnreachable,
}

impl ErrorCode {
    /// Every code, in table order.
    pub const ALL: [Self; 9] = [
        Self::Unauthenticated,
        Self::NotFound,
        Self::ResourceExhausted,
        Self::Conflict,
        Self::Exists,
        Self::VersionConflict,
        Self::ConflictClaimed,
        Self::LeaseLost,
        Self::DaemonUnreachable,
    ];

    /// Stable symbolic name used in logs and JSON-RPC `error.data.code`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::NotFound => "NOT_FOUND",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::Conflict => "CONFLICT",
            Self::Exists => "EXISTS",
            Self::VersionConflict => "VERSION_CONFLICT",
            Self::ConflictClaimed => "CONFLICT_CLAIMED",
            Self::LeaseLost => "LEASE_LOST",
            Self::DaemonUnreachable => "DAEMON_UNREACHABLE",
        }
    }

    /// JSON-RPC numeric code for this application error.
    pub const fn rpc_code(self) -> i64 {
        match self {
            Self::Unauthenticated => -32001,
            Self::NotFound => -32002,
            Self::ResourceExhausted => -32003,
            Self::Conflict => -32004,
            Self::Exists => -32005,
            Self::VersionConflict => -32006,
            Self::ConflictClaimed => -32007,
            Self::LeaseLost => -32008,
            Self::DaemonUnreachable => -32009,
        }
    }

    /// Human-readable default message.
    pub const fn message(self) -> &'static str {
        match self {
            Self::Unauthenticated => "authentication required",
            Self::NotFound => "not found",
            Self::ResourceExhausted => "resource exhausted",
            Self::Conflict => "conflict",
            Self::Exists => "already exists",
            Self::VersionConflict => "version conflict",
            Self::ConflictClaimed => "resource is claimed by another agent",
            Self::LeaseLost => "lease lost",
            Self::DaemonUnreachable => "daemon unreachable",
        }
    }

    /// Resolves a numeric JSON-RPC code back to its symbolic name.
    pub fn from_rpc_code(code: i64) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|candidate| candidate.rpc_code() == code)
    }

    /// Resolves a symbolic name such as `"NOT_FOUND"`.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|candidate| candidate.as_str() == name)
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
