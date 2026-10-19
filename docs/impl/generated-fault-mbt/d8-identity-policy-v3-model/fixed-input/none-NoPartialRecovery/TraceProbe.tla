--------------------------- MODULE TraceProbe ---------------------------
EXTENDS MBFaults, TLC
Expected == <<[action_taken |-> "init", observation |-> [phase |-> "ready", receipts |-> <<>>, records |-> <<>>, recoveryRequired |-> FALSE], parameters |-> [actor |-> "-", mode |-> "-", scenario |-> "storage", value |-> 0], result |-> "none"], [action_taken |-> "append-bytes", observation |-> [phase |-> "ready", receipts |-> <<>>, records |-> <<[id |-> 1, seq |-> 0]>>, recoveryRequired |-> TRUE], parameters |-> [actor |-> "-", mode |-> "partial", scenario |-> "storage", value |-> 1], result |-> "io-error"], [action_taken |-> "append-bytes", observation |-> [phase |-> "ready", receipts |-> <<>>, records |-> <<[id |-> 1, seq |-> 0]>>, recoveryRequired |-> TRUE], parameters |-> [actor |-> "-", mode |-> "whole", scenario |-> "storage", value |-> 2], result |-> "recovery-required"], [action_taken |-> "recover-storage", observation |-> [phase |-> "ready", receipts |-> <<>>, records |-> <<>>, recoveryRequired |-> FALSE], parameters |-> [actor |-> "-", mode |-> "retained-prefix", scenario |-> "storage", value |-> 1], result |-> "ok"], [action_taken |-> "append-bytes", observation |-> [phase |-> "written", receipts |-> <<>>, records |-> <<[id |-> 2, seq |-> 1]>>, recoveryRequired |-> FALSE], parameters |-> [actor |-> "-", mode |-> "whole", scenario |-> "storage", value |-> 2], result |-> "pending"], [action_taken |-> "sync-ok", observation |-> [phase |-> "ready", receipts |-> <<[id |-> 2, seq |-> 1]>>, records |-> <<[id |-> 2, seq |-> 1]>>, recoveryRequired |-> FALSE], parameters |-> [actor |-> "-", mode |-> "-", scenario |-> "storage", value |-> 0], result |-> "ok"], [action_taken |-> "inspect-storage", observation |-> [phase |-> "ready", receipts |-> <<[id |-> 2, seq |-> 1]>>, records |-> <<[id |-> 2, seq |-> 1]>>, recoveryRequired |-> FALSE], parameters |-> [actor |-> "-", mode |-> "-", scenario |-> "storage", value |-> 0], result |-> "ok"]>>
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
