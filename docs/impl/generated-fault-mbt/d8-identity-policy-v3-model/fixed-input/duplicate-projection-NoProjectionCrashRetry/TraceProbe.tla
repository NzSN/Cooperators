--------------------------- MODULE TraceProbe ---------------------------
EXTENDS MBFaults, TLC
Expected == <<[action_taken |-> "init", observation |-> [deadRecords |-> <<>>, terminal |-> FALSE, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "-", scenario |-> "deadletter", value |-> 0], result |-> "none"], [action_taken |-> "mark-dead", observation |-> [deadRecords |-> <<>>, terminal |-> TRUE, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "durable-terminal", scenario |-> "deadletter", value |-> 1], result |-> "pending"], [action_taken |-> "project-dead", observation |-> [deadRecords |-> <<1>>, terminal |-> TRUE, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "dedupe-by-terminal", scenario |-> "deadletter", value |-> 1], result |-> "written"], [action_taken |-> "crash-dead", observation |-> [deadRecords |-> <<1>>, terminal |-> TRUE, up |-> FALSE], parameters |-> [actor |-> "-", mode |-> "-", scenario |-> "deadletter", value |-> 0], result |-> "stopped"], [action_taken |-> "recover-dead", observation |-> [deadRecords |-> <<1>>, terminal |-> TRUE, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "reconcile-terminal-records", scenario |-> "deadletter", value |-> 0], result |-> "ok"], [action_taken |-> "project-dead", observation |-> [deadRecords |-> <<1>>, terminal |-> TRUE, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "dedupe-by-terminal", scenario |-> "deadletter", value |-> 1], result |-> "already-projected"]>>
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
