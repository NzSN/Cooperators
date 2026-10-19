# Cooperators

Cooperators coordinates independently running agent sessions through a local
broker. Agent identity belongs to an individual session.

## Language

**Agent instance**:
One independently launched agent session, such as one Codex session or one
Claude session. Separate sessions remain separate instances even when they use
the same application and configuration.
_Avoid_: agent kind, agent name

**Agent identity**:
The unique identity of one agent instance throughout that session, including
its connection interruptions and reconnects.
_Avoid_: application name, display name

**Agent kind**:
The application family of an agent instance, such as Codex or Claude. Many
instances can have the same kind.
_Avoid_: identity

**Agent name**:
A human-readable label or alias for an agent instance. Its wording does not
establish which instance owns an identity.
_Avoid_: credential, identity

**Broker instance**:
One running daemon that serves a bus root and owns its durable writes.
_Avoid_: agent instance
