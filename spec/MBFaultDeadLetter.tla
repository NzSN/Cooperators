------------------------- MODULE MBFaultDeadLetter -------------------------
(* Generated interface view of MBFaults, scenario deadletter. *)
(* Declaration-only public projection; MBFaults remains the transition oracle. *)
(* No replacement transition algorithm is defined or verified here. *)
(* Canonical MBFaults SHA256: 117e081f6f540f628862c7c77dbfe1f41705a732abeee91c68a4721096c3222a *)
EXTENDS Integers, Sequences, FiniteSets
VARIABLES
  \* @type: {terminal: Bool, deadRecords: Seq(Int), up: Bool};
  observation,
  \* @type: Str;
  result,
  \* @type: {actor: Str, mode: Str, scenario: Str, value: Int};
  parameters,
  \* @type: Str;
  action_taken
=============================================================================
