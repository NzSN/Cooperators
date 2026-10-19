----------------------- MODULE CooperatorsInstances -----------------------
(* Composition of the existing identity lifecycle and broker protocol.      *)
(* Requests authenticate before effects; labels/kinds confer no authority.   *)
(* The transition monitors evaluate both original Next relations against the *)
(* actual old/new states. This checks safety refinement, not either model's  *)
(* fairness/liveness assumptions or the Rust implementation.                *)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Instances, AgentIds, Tokens, Keys, Kind, DisplayName,
          MaxAttempts, Q, D, MaxSteps, Scenario, Fault
Scenarios == {"messaging", "queue", "documents", "combined"}
Faults == {"none", "missing-auth", "revoked-token", "sender-spoof", "foreign-ack",
           "kind-dedupe", "foreign-task", "foreign-doc", "cas-bypass", "stale-fence",
           "crash-forgets-identity", "bad-ack", "identity-rebind"}
ASSUME /\ Scenario \in Scenarios /\ Fault \in Faults /\ MaxSteps >= 1

VARIABLES identity, identityClock, log, ackCursor, steps, last,
          brokerRefines, identityRefines, authenticated, actorBound, decisionsCorrect,
          ackIsolated, parametersChecked, crashKeepsIdentity, recipientLive, crashedWithMessage
vars == <<identity, identityClock, log, ackCursor, steps, last,
          brokerRefines, identityRefines, authenticated, actorBound, decisionsCorrect,
          ackIsolated, parametersChecked, crashKeepsIdentity, recipientLive, crashedWithMessage>>

RegistryFault == IF Fault = "identity-rebind" THEN "reconnect-id" ELSE "none"
Registry == INSTANCE AgentInstances WITH
  Instances <- Instances, AgentIds <- AgentIds, Tokens <- Tokens, Keys <- Keys,
  Kind <- Kind, DisplayName <- DisplayName, Scenario <- "lifecycle", Fault <- RegistryFault,
  st <- identity, steps <- identityClock
Identity == INSTANCE AgentInstances WITH
  Instances <- Instances, AgentIds <- AgentIds, Tokens <- Tokens, Keys <- Keys,
  Kind <- Kind, DisplayName <- DisplayName, Scenario <- "lifecycle", Fault <- "none",
  st <- identity, steps <- identityClock
Broker == INSTANCE Cooperators WITH
  Agents <- AgentIds, Keys <- Keys, MaxAttempts <- MaxAttempts, Q <- Q, D <- D,
  log <- log, ackCursor <- ackCursor

NoId == Identity!NoId
NoToken == Identity!NoToken
LiveIds == {identity.id[i] : i \in identity.active}
ConnectedIds == {identity.id[i] : i \in identity.connected}
Holder(r) == IF Broker!ActiveFence(r) = 0 THEN NoId ELSE Broker!ActiveHolder(r)
Principal(t) == Identity!Resolve(t)
AuthOK(t) == Principal(t) \in LiveIds /\ Principal(t) \in ConnectedIds
HeldTokens(i) == {NoToken} \cup {t \in identity.usedTokens : identity.tokenInstance[t] = i}
QueueTerms == {"complete-task", "fail-task"}
DocEdits == {"update-doc", "release-doc"}
UserOps == {"send", "ack", "claim-task", "complete-task", "fail-task",
            "publish-doc", "claim-doc", "update-doc", "release-doc"}
Decisions == {"none", "apply", "duplicate", "noop", "unauthenticated", "not-found",
              "not-owner", "lease-lost", "version-conflict", "exists",
              "conflict-claimed", "empty", "bad-cursor", "bad-op"}
Accepted(d) == d \in {"apply", "duplicate", "noop"}
EmptyLast(op) == [op |-> op, expected |-> "none", actual |-> "none",
  principal |-> NoId, token |-> NoToken, target |-> NoId, base |-> 0, fence |-> 0, cursor |-> 0]

Init == /\ Identity!Init /\ Broker!Init /\ steps = 0 /\ last = EmptyLast("init")
        /\ brokerRefines = TRUE /\ identityRefines = TRUE /\ authenticated = TRUE
        /\ actorBound = TRUE /\ decisionsCorrect = TRUE /\ ackIsolated = TRUE
        /\ parametersChecked = TRUE /\ crashKeepsIdentity = TRUE /\ recipientLive = TRUE
        /\ crashedWithMessage = FALSE

(* Pure policy, shared by the correct and deliberately faulty request paths. *)
Decision(op, p, target, key, base, fence, cursor, auth, badAck, badDedupe, badCAS, badFence) ==
  IF ~auth THEN "unauthenticated"
  ELSE CASE op = "send" ->
       IF target \notin LiveIds THEN "not-found"
       ELSE IF <<p, key>> \in Broker!UsedPairs \/
               (badDedupe /\ \E n \in DOMAIN log : log[n].type = "msg" /\ log[n].key = key)
            THEN "duplicate" ELSE "apply"
    [] op = "ack" ->
       IF target # p /\ ~badAck THEN "not-owner"
       ELSE IF cursor > Broker!MaxMsgSeqTo(target) THEN "bad-cursor"
       ELSE IF cursor <= ackCursor[target] THEN "noop" ELSE "apply"
    [] op \in {"claim-task", "claim-doc"} ->
       LET r == IF op = "claim-task" THEN Q ELSE D
       IN IF r = D /\ Broker!Version = 0 THEN "not-found"
          ELSE IF Broker!ActiveFence(r) # 0 THEN "conflict-claimed"
          ELSE IF r = Q /\ (Broker!Dead \/ Broker!Completed) THEN "empty" ELSE "apply"
    [] op \in QueueTerms \cup {"release-doc"} ->
       LET r == IF op \in QueueTerms THEN Q ELSE D
       IN IF Broker!ActiveFence(r) = 0 \/ Holder(r) # p THEN "lease-lost"
          ELSE IF fence # Broker!ActiveFence(r) /\ ~badFence THEN "lease-lost" ELSE "apply"
    [] op = "publish-doc" -> IF Broker!Version # 0 THEN "exists" ELSE "apply"
    [] op = "update-doc" ->
       IF Broker!Version = 0 THEN "not-found"
       ELSE IF Broker!ActiveFence(D) # 0 /\ Holder(D) # p THEN "conflict-claimed"
       ELSE IF fence # Broker!ActiveFence(D) /\ ~badFence THEN "lease-lost"
       ELSE IF base # Broker!Version /\ ~badCAS THEN "version-conflict" ELSE "apply"
    [] OTHER -> "bad-op"

ActualPrincipal(t) ==
  IF Fault = "revoked-token" /\ t \in identity.revoked THEN identity.tokenOwner[t]
  ELSE IF Fault = "missing-auth" /\ Principal(t) = NoId
       THEN CHOOSE a \in AgentIds : TRUE ELSE Principal(t)
ActualAuth(t) ==
  IF Fault = "missing-auth" THEN TRUE
  ELSE IF Fault = "revoked-token" /\ t \in identity.revoked
       THEN ActualPrincipal(t) \in AgentIds ELSE AuthOK(t)
ActualActor(op, t) ==
  LET p == ActualPrincipal(t)
      others == LiveIds \ {p}
  IN IF Fault = "sender-spoof" /\ op = "send" /\ others # {}
     THEN CHOOSE a \in others : TRUE
     ELSE IF Fault = "foreign-task" /\ op \in QueueTerms /\ Holder(Q) # NoId THEN Holder(Q)
     ELSE IF Fault = "foreign-doc" /\ op \in DocEdits /\ Holder(D) # NoId THEN Holder(D)
     ELSE p

Apply(op, p, target, key, cursor) ==
  CASE op = "send" -> Broker!Send(p, target, key)
    [] op = "ack" ->
       IF Fault = "bad-ack"
       THEN /\ ackCursor' = [ackCursor EXCEPT ![target] = cursor + 1] /\ UNCHANGED log
       ELSE /\ Broker!Ack(target) /\ ackCursor'[target] = cursor
    [] op = "claim-task" -> Broker!Claim(Q, p)
    [] op = "complete-task" -> Broker!Complete(p)
    [] op = "fail-task" -> Broker!Fail(p)
    [] op = "publish-doc" -> Broker!Publish(p)
    [] op = "claim-doc" -> Broker!Claim(D, p)
    [] op = "update-doc" -> Broker!Update(p)
    [] op = "release-doc" -> Broker!ReleaseD(p)

(* The actual projected next states are already bound when Stamp evaluates   *)
(* these action formulas. Stuttering is checked first to keep monitoring cheap. *)
Stamp ==
  /\ brokerRefines' = (brokerRefines /\ ((UNCHANGED <<log, ackCursor>>) \/ Broker!Next))
  /\ identityRefines' = (identityRefines /\
        ((UNCHANGED <<identity, identityClock>>) \/ Identity!Next))
  /\ steps' = steps + 1

Lifecycle ==
  /\ Registry!Next /\ UNCHANGED <<log, ackCursor>> /\ last' = EmptyLast("lifecycle")
  /\ UNCHANGED <<authenticated, actorBound, decisionsCorrect, ackIsolated,
                 parametersChecked, crashKeepsIdentity, recipientLive, crashedWithMessage>>
  /\ Stamp

Request(op, t, target, key, base, fence, cursor) ==
  LET p == Principal(t)
      actor == ActualActor(op, t)
      expected == Decision(op, p, target, key, base, fence, cursor,
                           AuthOK(t), FALSE, FALSE, FALSE, FALSE)
      actual == Decision(op, actor, target, key, base, fence, cursor, ActualAuth(t),
                         Fault = "foreign-ack", Fault = "kind-dedupe",
                         Fault = "cas-bypass", Fault = "stale-fence")
  IN /\ IF actual = "apply" THEN Apply(op, actor, target, key, cursor)
         ELSE UNCHANGED <<log, ackCursor>>
     /\ UNCHANGED <<identity, identityClock, crashKeepsIdentity, crashedWithMessage>>
     /\ last' = [op |-> op, expected |-> expected, actual |-> actual,
          principal |-> p, token |-> t,
          target |-> IF op \in {"send", "ack"} /\ AuthOK(t) THEN target ELSE NoId,
          base |-> IF op = "update-doc" /\ AuthOK(t) THEN base ELSE 0,
          fence |-> IF op \in QueueTerms \cup DocEdits /\ AuthOK(t) THEN fence ELSE 0,
          cursor |-> IF op = "ack" /\ AuthOK(t) THEN cursor ELSE 0]
     /\ authenticated' = (authenticated /\ (Accepted(actual) => AuthOK(t)))
     /\ actorBound' = (actorBound /\ (Accepted(actual) => actor = p))
     /\ decisionsCorrect' = (decisionsCorrect /\ actual = expected)
     /\ ackIsolated' = (ackIsolated /\ (op = "ack" =>
          \A a \in AgentIds \ {p} : ackCursor'[a] = ackCursor[a]))
     /\ parametersChecked' = (parametersChecked /\ (actual = "apply" =>
          /\ (op = "ack" => ackCursor'[target] = cursor)
          /\ (op \in QueueTerms => fence = Broker!ActiveFence(Q))
          /\ (op \in DocEdits => fence = Broker!ActiveFence(D))
          /\ (op = "update-doc" => base = Broker!Version)))
     /\ recipientLive' = (recipientLive /\
          (op = "send" /\ Accepted(actual) => target \in LiveIds))
     /\ Stamp

FenceOptions(r) == {0, Broker!ActiveFence(r),
                    IF Broker!ActiveFence(r) = 0 THEN 0 ELSE Broker!ActiveFence(r) - 1}
RequestNext == \E i \in Instances : \E t \in HeldTokens(i) :
  \/ /\ Scenario \in {"messaging", "combined"}
     /\ (\E to \in AgentIds, k \in Keys : Request("send", t, to, k, 0, 0, 0))
  \/ /\ Scenario \in {"messaging", "combined"}
     /\ (\E a \in AgentIds : \E c \in 0..(Broker!MaxMsgSeqTo(a) + 1) :
          Request("ack", t, a, CHOOSE k \in Keys : TRUE, 0, 0, c))
  \/ /\ Scenario \in {"queue", "combined"}
     /\ (Request("claim-task", t, NoId, CHOOSE k \in Keys : TRUE, 0, 0, 0)
         \/ (\E op \in QueueTerms, f \in FenceOptions(Q) :
              Request(op, t, NoId, CHOOSE k \in Keys : TRUE, 0, f, 0)))
  \/ /\ Scenario \in {"documents", "combined"}
     /\ (\E op \in {"publish-doc", "claim-doc"} :
            Request(op, t, NoId, CHOOSE k \in Keys : TRUE, 0, 0, 0))
  \/ /\ Scenario \in {"documents", "combined"}
     /\ (\E b \in {Broker!Version, IF Broker!Version = 0 THEN 1 ELSE Broker!Version - 1},
             f \in FenceOptions(D) :
           Request("update-doc", t, NoId, CHOOSE k \in Keys : TRUE, b, f, 0))
  \/ /\ Scenario \in {"documents", "combined"}
     /\ (\E f \in FenceOptions(D) :
           Request("release-doc", t, NoId, CHOOSE k \in Keys : TRUE, 0, f, 0))

Expire(r) ==
  /\ Broker!Expire(r) /\ UNCHANGED <<identity, identityClock>>
  /\ last' = EmptyLast(IF r = Q THEN "expire-task" ELSE "expire-doc")
  /\ UNCHANGED <<authenticated, actorBound, decisionsCorrect, ackIsolated,
                 parametersChecked, crashKeepsIdentity, recipientLive, crashedWithMessage>>
  /\ Stamp
Crash ==
  /\ Broker!Crash
  /\ IF Fault = "crash-forgets-identity"
     THEN /\ identity' = [identity EXCEPT !.id = [i \in Instances |-> NoId],
                  !.active = {}, !.connected = {}, !.validTokens = {}]
          /\ UNCHANGED identityClock
     ELSE UNCHANGED <<identity, identityClock>>
  /\ last' = EmptyLast("crash")
  /\ crashKeepsIdentity' = (crashKeepsIdentity /\ identity' = identity)
  /\ crashedWithMessage' = (crashedWithMessage \/ (Cardinality(LiveIds) = 2
       /\ \E n \in DOMAIN log : log[n].type = "msg"))
  /\ UNCHANGED <<authenticated, actorBound, decisionsCorrect, ackIsolated,
                 parametersChecked, recipientLive>>
  /\ Stamp
Next == /\ steps < MaxSteps
        /\ (Lifecycle \/ RequestNext \/ Crash
            \/ (Scenario \in {"queue", "combined"} /\ Expire(Q))
            \/ (Scenario \in {"documents", "combined"} /\ Expire(D)))
Spec == Init /\ [][Next]_vars

TypeOK == /\ Identity!TypeOK /\ Broker!TypeOK /\ steps \in 0..MaxSteps
  /\ Len(log) <= MaxSteps /\ \A a \in AgentIds : ackCursor[a] <= MaxSteps + 1
  /\ \A flag \in {brokerRefines, identityRefines, authenticated, actorBound,
       decisionsCorrect, ackIsolated, parametersChecked, crashKeepsIdentity, recipientLive,
       crashedWithMessage} : flag \in BOOLEAN
  /\ last \in [op : UserOps \cup {"init", "lifecycle", "crash", "expire-task", "expire-doc"},
       expected : Decisions, actual : Decisions, principal : AgentIds \cup {NoId},
       token : Tokens \cup {NoToken}, target : AgentIds \cup {NoId},
       base : 0..(MaxSteps + 1), fence : 0..(MaxSteps + 1), cursor : 0..(MaxSteps + 1)]
ProtocolInvariants == /\ Broker!NoDuplicateMessages /\ Broker!ClaimSeparation
  /\ Broker!FenceFreshness /\ Broker!AttemptsBound /\ Broker!DeadMeansNoActiveLease
  /\ Broker!DocVersionChain /\ Broker!DocHolderRespected /\ Broker!DocWriteFenceLive
InstanceInvariants == /\ Identity!InstanceIdentityUnique /\ Identity!IdentityNotReused
  /\ Identity!EnrollmentRetryStable /\ Identity!InstanceIdentityStable
  /\ Identity!CredentialBoundToInstance /\ Identity!DistinctInstanceCredentials
  /\ Identity!RevokedCredentialsRejected
BrokerSafetyRefinement == brokerRefines
IdentitySafetyRefinement == identityRefines
AuthenticatedRequestsOnly == authenticated
CanonicalActorBinding == actorBound
RequestDecisionsCorrect == decisionsCorrect
AckIsolation == ackIsolated
RequestParametersChecked == parametersChecked
CrashIdentityPreserved == crashKeepsIdentity
LiveRecipientsOnly == recipientLive

(* Deliberately false coverage goals, not broker safety requirements. *)
NoSameKindExchange == ~(Cardinality(LiveIds) = 2 /\
  \E a, b \in LiveIds, k \in Keys : a # b /\
    (\E m \in DOMAIN log : log[m].type = "msg" /\ log[m].from = a
         /\ log[m].to = b /\ log[m].key = k) /\
    (\E n \in DOMAIN log : log[n].type = "msg" /\ log[n].from = b
         /\ log[n].to = a /\ log[n].key = k))
NoForeignTaskRejection == ~(last.op = "complete-task" /\ last.actual = "lease-lost"
  /\ last.principal \in LiveIds /\ Holder(Q) # NoId /\ last.principal # Holder(Q))
NoForeignDocRejection == ~(last.op = "update-doc" /\ last.actual = "conflict-claimed"
  /\ last.principal \in LiveIds /\ Holder(D) # NoId /\ last.principal # Holder(D))
NoRotatedDocWrite == ~(last.op = "update-doc" /\ last.actual = "apply"
  /\ last.principal = Holder(D) /\ Broker!ActiveFence(D) # 0 /\ Cardinality(LiveIds) = 2
  /\ \E t \in identity.revoked : identity.tokenOwner[t] = last.principal)
NoCrashAndResume == ~(last.op = "ack" /\ last.actual = "apply"
  /\ crashedWithMessage /\ Cardinality(LiveIds) = 2
  /\ \E a \in LiveIds : ackCursor[a] > 0)
NoCombinedProtocol == ~(Cardinality(LiveIds) = 2 /\ Broker!Version > 0
  /\ Broker!ActiveFence(Q) # 0 /\ \E n \in DOMAIN log : log[n].type = "msg")

(* Three independent model-value spaces; kind and display text stay fixed. *)
Symmetry == { [x \in Instances \cup AgentIds \cup Tokens |->
  IF x \in Instances THEN pi[x] ELSE IF x \in AgentIds THEN pa[x] ELSE pt[x]] :
  pi \in Permutations(Instances), pa \in Permutations(AgentIds), pt \in Permutations(Tokens) }
===========================================================================
