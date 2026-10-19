------------------------- MODULE MBFaultWriter -------------------------
(* Generated interface view of MBFaults, scenario writer. *)
(* Declaration-only public projection; MBFaults remains the transition oracle. *)
(* No replacement transition algorithm is defined or verified here. *)
(* Canonical MBFaults SHA256: 8dfb916fe2c2f69516ecaf11da3e31596167818fdcc94859afb7e676ea6fc19c *)
EXTENDS Integers, Sequences, FiniteSets
VARIABLES
  \* @type: {owners: Set(Str), records: Seq(Int), rejected: Set(Str), torn: Bool};
  observation,
  \* @type: Str;
  result,
  \* @type: {actor: Str, mode: Str, scenario: Str, value: Int};
  parameters,
  \* @type: Str;
  action_taken
=============================================================================
