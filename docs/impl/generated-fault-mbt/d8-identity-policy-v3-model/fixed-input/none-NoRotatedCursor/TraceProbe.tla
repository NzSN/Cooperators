--------------------------- MODULE TraceProbe ---------------------------
EXTENDS MBFaults, TLC
Expected == <<[action_taken |-> "init", observation |-> [cursor |-> [file |-> 1, seq |-> 0], files |-> <<<<>>>>, readBack |-> <<>>, records |-> <<>>, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "-", scenario |-> "rotation", value |-> 0], result |-> "none"], [action_taken |-> "begin-rotating-append", observation |-> [cursor |-> [file |-> 1, seq |-> 0], files |-> <<<<>>>>, readBack |-> <<>>, records |-> <<>>, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "encoded-record-units", scenario |-> "rotation", value |-> 3], result |-> "pending"], [action_taken |-> "prepare-segment", observation |-> [cursor |-> [file |-> 1, seq |-> 0], files |-> <<<<>>>>, readBack |-> <<>>, records |-> <<>>, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "before-record-commit", scenario |-> "rotation", value |-> 0], result |-> "pending"], [action_taken |-> "commit-rotating-append", observation |-> [cursor |-> [file |-> 1, seq |-> 0], files |-> <<<<1>>>>, readBack |-> <<>>, records |-> <<[bytes |-> 3, id |-> 1]>>, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "-", scenario |-> "rotation", value |-> 0], result |-> "ok"], [action_taken |-> "save-cursor", observation |-> [cursor |-> [file |-> 1, seq |-> 1], files |-> <<<<1>>>>, readBack |-> <<>>, records |-> <<[bytes |-> 3, id |-> 1]>>, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "logical-sequence", scenario |-> "rotation", value |-> 0], result |-> "ok"], [action_taken |-> "begin-rotating-append", observation |-> [cursor |-> [file |-> 1, seq |-> 1], files |-> <<<<1>>>>, readBack |-> <<>>, records |-> <<[bytes |-> 3, id |-> 1]>>, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "encoded-record-units", scenario |-> "rotation", value |-> 1], result |-> "pending"], [action_taken |-> "prepare-segment", observation |-> [cursor |-> [file |-> 1, seq |-> 1], files |-> <<<<1>>, <<>>>>, readBack |-> <<>>, records |-> <<[bytes |-> 3, id |-> 1]>>, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "before-record-commit", scenario |-> "rotation", value |-> 0], result |-> "pending"], [action_taken |-> "commit-rotating-append", observation |-> [cursor |-> [file |-> 1, seq |-> 1], files |-> <<<<1>>, <<2>>>>, readBack |-> <<>>, records |-> <<[bytes |-> 3, id |-> 1], [bytes |-> 1, id |-> 2]>>, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "-", scenario |-> "rotation", value |-> 0], result |-> "ok"], [action_taken |-> "begin-rotating-append", observation |-> [cursor |-> [file |-> 1, seq |-> 1], files |-> <<<<1>>, <<2>>>>, readBack |-> <<>>, records |-> <<[bytes |-> 3, id |-> 1], [bytes |-> 1, id |-> 2]>>, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "encoded-record-units", scenario |-> "rotation", value |-> 3], result |-> "pending"], [action_taken |-> "prepare-segment", observation |-> [cursor |-> [file |-> 1, seq |-> 1], files |-> <<<<1>>, <<2>>, <<>>>>, readBack |-> <<>>, records |-> <<[bytes |-> 3, id |-> 1], [bytes |-> 1, id |-> 2]>>, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "before-record-commit", scenario |-> "rotation", value |-> 0], result |-> "pending"], [action_taken |-> "commit-rotating-append", observation |-> [cursor |-> [file |-> 1, seq |-> 1], files |-> <<<<1>>, <<2>>, <<3>>>>, readBack |-> <<>>, records |-> <<[bytes |-> 3, id |-> 1], [bytes |-> 1, id |-> 2], [bytes |-> 3, id |-> 3]>>, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "-", scenario |-> "rotation", value |-> 0], result |-> "ok"], [action_taken |-> "read-cursor", observation |-> [cursor |-> [file |-> 1, seq |-> 1], files |-> <<<<1>>, <<2>>, <<3>>>>, readBack |-> <<2, 3>>, records |-> <<[bytes |-> 3, id |-> 1], [bytes |-> 1, id |-> 2], [bytes |-> 3, id |-> 3]>>, up |-> TRUE], parameters |-> [actor |-> "-", mode |-> "all-segments", scenario |-> "rotation", value |-> 1], result |-> "ok"]>>
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
