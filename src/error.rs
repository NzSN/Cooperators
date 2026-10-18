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

/// Error type returned by broker services and mapped at the JSON-RPC edge.
///
/// Application failures carry a canonical [`ErrorCode`]; invalid parameters
/// and internal I/O failures are protocol-level or server-side errors.
#[derive(Debug)]
pub enum BrokerError {
    /// A canonical broker error with a human-readable detail string.
    Application(ErrorCode, String),
    /// The request parameters were malformed or failed validation.
    Invalid(String),
    /// An unexpected server-side failure.
    Internal(String),
    /// An underlying filesystem failure.
    Io(std::io::Error),
}

impl BrokerError {
    /// Creates an application error.
    pub fn application(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self::Application(code, detail.into())
    }

    /// Creates an invalid-parameter error.
    pub fn invalid(detail: impl Into<String>) -> Self {
        Self::Invalid(detail.into())
    }

    /// Creates an internal error.
    pub fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(detail.into())
    }

    /// Maps this error onto the JSON-RPC wire representation.
    pub fn into_rpc_error(self) -> crate::rpc::RpcError {
        match self {
            Self::Application(code, detail) => crate::rpc::RpcError::application(code, detail),
            Self::Invalid(detail) => crate::rpc::RpcError::invalid_params(detail),
            Self::Internal(detail) => crate::rpc::RpcError::internal(detail),
            Self::Io(error) => crate::rpc::RpcError::internal(error.to_string()),
        }
    }
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Application(code, detail) => write!(formatter, "{code}: {detail}"),
            Self::Invalid(detail) => write!(formatter, "invalid parameters: {detail}"),
            Self::Internal(detail) => write!(formatter, "internal error: {detail}"),
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
        }
    }
}

impl std::error::Error for BrokerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for BrokerError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<crate::storage::LogError> for BrokerError {
    fn from(error: crate::storage::LogError) -> Self {
        match error {
            crate::storage::LogError::Io(error) => Self::Io(error),
            other => Self::Internal(other.to_string()),
        }
    }
}
