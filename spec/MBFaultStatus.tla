------------------------- MODULE MBFaultStatus -------------------------
(* Generated interface view of MBFaults, scenario status. *)
(* Declaration-only public projection; MBFaults remains the transition oracle. *)
(* No replacement transition algorithm is defined or verified here. *)
(* Canonical MBFaults SHA256: 117e081f6f540f628862c7c77dbfe1f41705a732abeee91c68a4721096c3222a *)
EXTENDS Integers, Sequences, FiniteSets
VARIABLES
  \* @type: {records: Seq({id: Int, bytes: Int}), reported: Int};
  observation,
  \* @type: Str;
  result,
  \* @type: {actor: Str, mode: Str, scenario: Str, value: Int};
  parameters,
  \* @type: Str;
  action_taken
=============================================================================
