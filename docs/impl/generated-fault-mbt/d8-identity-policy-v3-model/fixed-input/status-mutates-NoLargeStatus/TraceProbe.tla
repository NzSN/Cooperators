--------------------------- MODULE TraceProbe ---------------------------
EXTENDS MBFaults, TLC
Expected == <<[action_taken |-> "init", observation |-> [records |-> <<>>, reported |-> -1], parameters |-> [actor |-> "-", mode |-> "-", scenario |-> "status", value |-> 0], result |-> "none"], [action_taken |-> "append-status", observation |-> [records |-> <<[bytes |-> 3, id |-> 1]>>, reported |-> -1], parameters |-> [actor |-> "-", mode |-> "encoded-record-units", scenario |-> "status", value |-> 3], result |-> "ok"], [action_taken |-> "sample-status", observation |-> [records |-> <<[bytes |-> 3, id |-> 1]>>, reported |-> 1], parameters |-> [actor |-> "-", mode |-> "read-only", scenario |-> "status", value |-> 0], result |-> "ok"]>>
TraceConforms == /\ observation = Expected[steps + 1].observation
                 /\ result = Expected[steps + 1].result
(* Fix only genuinely fresh allocation choices as test fixture entropy. A
   faulty reuse is not fresh and is NOT filtered out. Reconnect ID changes
   are also not filtered. Neither guiding condition is sent to a SUT port. *)
FreshChoices == IF Scenario = "identity" THEN
  LET actor == parameters'.actor
      row == Expected[steps + 2].observation
  IN IF action_taken' = "enroll" /\ actor \notin s.enrolled
     THEN /\ (s'.id[actor] \in AgentIds \ s.usedIds => s'.id[actor] = row.ids[actor])
          /\ (s'.currentToken[actor] \in Tokens \ s.usedTokens =>
                s'.currentToken[actor] = row.currentTokens[actor])
     ELSE IF action_taken' = "rotate-token"
          THEN s'.currentToken[actor] = row.currentTokens[actor] ELSE TRUE
  ELSE TRUE
ProbeNext == IF steps < Len(Expected) - 1
  THEN /\ Next
       /\ action_taken' = Expected[steps + 2].action_taken
       /\ parameters' = Expected[steps + 2].parameters
       /\ FreshChoices
  ELSE UNCHANGED vars
ProbeSpec == Init /\ [][ProbeNext]_vars
===========================================================================
