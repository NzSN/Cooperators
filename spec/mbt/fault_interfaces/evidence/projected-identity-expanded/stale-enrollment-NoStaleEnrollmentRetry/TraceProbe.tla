--------------------------- MODULE TraceProbe ---------------------------
EXTENDS MBFaults, TLC
Expected == <<[action_taken |-> "init", observation |-> [active |-> {}, connected |-> {}, currentTokens |-> ("launch1" :> "-token" @@ "launch2" :> "-token"), ids |-> ("launch1" :> "-id" @@ "launch2" :> "-id"), kinds |-> ("launch1" :> "codex" @@ "launch2" :> "codex"), labels |-> ("launch1" :> "codex" @@ "launch2" :> "codex"), replies |-> ("launch1" :> "-id" @@ "launch2" :> "-id")], parameters |-> [actor |-> "-", mode |-> "-", scenario |-> "identity", value |-> 0], result |-> "none"], [action_taken |-> "enroll", observation |-> [active |-> {"launch1"}, connected |-> {"launch1"}, currentTokens |-> ("launch1" :> "token1" @@ "launch2" :> "-token"), ids |-> ("launch1" :> "id1" @@ "launch2" :> "-id"), kinds |-> ("launch1" :> "codex" @@ "launch2" :> "codex"), labels |-> ("launch1" :> "codex" @@ "launch2" :> "codex"), replies |-> ("launch1" :> "id1" @@ "launch2" :> "-id")], parameters |-> [actor |-> "launch1", mode |-> "fresh-launch-key", scenario |-> "identity", value |-> 0], result |-> "ok"], [action_taken |-> "enroll", observation |-> [active |-> {"launch1", "launch2"}, connected |-> {"launch1", "launch2"}, currentTokens |-> ("launch1" :> "token1" @@ "launch2" :> "token2"), ids |-> ("launch1" :> "id1" @@ "launch2" :> "id2"), kinds |-> ("launch1" :> "codex" @@ "launch2" :> "codex"), labels |-> ("launch1" :> "codex" @@ "launch2" :> "codex"), replies |-> ("launch1" :> "id1" @@ "launch2" :> "id2")], parameters |-> [actor |-> "launch2", mode |-> "fresh-launch-key", scenario |-> "identity", value |-> 0], result |-> "ok"], [action_taken |-> "rotate-token", observation |-> [active |-> {"launch1", "launch2"}, connected |-> {"launch1", "launch2"}, currentTokens |-> ("launch1" :> "token1" @@ "launch2" :> "token3"), ids |-> ("launch1" :> "id1" @@ "launch2" :> "id2"), kinds |-> ("launch1" :> "codex" @@ "launch2" :> "codex"), labels |-> ("launch1" :> "codex" @@ "launch2" :> "codex"), replies |-> ("launch1" :> "id1" @@ "launch2" :> "id2")], parameters |-> [actor |-> "launch2", mode |-> "-", scenario |-> "identity", value |-> 0], result |-> "ok"], [action_taken |-> "enroll", observation |-> [active |-> {"launch1", "launch2"}, connected |-> {"launch1", "launch2"}, currentTokens |-> ("launch1" :> "token1" @@ "launch2" :> "token3"), ids |-> ("launch1" :> "id1" @@ "launch2" :> "id2"), kinds |-> ("launch1" :> "codex" @@ "launch2" :> "codex"), labels |-> ("launch1" :> "codex" @@ "launch2" :> "codex"), replies |-> ("launch1" :> "id1" @@ "launch2" :> "id2")], parameters |-> [actor |-> "launch2", mode |-> "fresh-launch-key", scenario |-> "identity", value |-> 0], result |-> "conflict"]>>
RuntimeObservation == IF Scenario = "storage"
  THEN [records |-> observation.records, receipts |-> observation.receipts]
  ELSE observation
TraceConforms == /\ RuntimeObservation = Expected[steps + 1].observation
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
