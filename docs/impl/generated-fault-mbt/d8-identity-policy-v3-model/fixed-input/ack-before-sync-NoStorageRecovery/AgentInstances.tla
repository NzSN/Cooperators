-------------------------- MODULE AgentInstances --------------------------
(* Agent identity is per independent launch, not per kind, label, socket,   *)
(* or broker process. Instance slots represent distinct launch request keys. *)
(* Both slots deliberately share kind/label in the supplied configuration.  *)
(* The broker assigns opaque IDs and credentials. Allocation/authentication *)
(* are atomic here; filesystem durability and root ownership remain separate *)
(* obligations. Token secrecy, entropy, and launch-key provisioning are not  *)
(* proved. Ghost fields remember first allocations and authorized effects.   *)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
  \* @type: Set(Str);
  Instances,
  \* @type: Set(Str);
  AgentIds,
  \* @type: Set(Str);
  Tokens,
  \* @type: Set(Str);
  Keys,
  \* @type: Str;
  Kind,
  \* @type: Str;
  DisplayName,
  \* @type: Str;
  Scenario,
  \* @type: Str;
  Fault

Scenarios == {"lifecycle", "messaging", "presence", "lease", "combined"}
Faults == {"none", "kind-identity", "reused-id", "retry-id", "reconnect-id",
           "shared-token", "stale-token", "inbox-broadcast", "foreign-ack",
           "kind-dedupe", "group-heartbeat", "foreign-lease", "stale-fence"}
NoId == "-id"
NoInstance == "-instance"
NoToken == "-token"
MaxMessages == 2
MaxClaims == 2
MaxTransitions == 6
ASSUME /\ Scenario \in Scenarios /\ Fault \in Faults
       /\ NoId \notin AgentIds /\ NoInstance \notin Instances /\ NoToken \notin Tokens
       /\ Cardinality(Instances) >= 2 /\ Cardinality(AgentIds) >= 2

VARIABLES
  (* @type: {enrolled: Set(Str), active: Set(Str), connected: Set(Str),
    kind: Str -> Str, label: Str -> Str, id: Str -> Str, firstId: Str -> Str,
    reply: Str -> Str, currentToken: Str -> Str, usedIds: Set(Str),
    issuedOwner: Str -> Str, usedTokens: Set(Str), tokenOwner: Str -> Str,
    tokenInstance: Str -> Str, validTokens: Set(Str), revoked: Set(Str),
    messages: Seq({from: Str, to: Str, key: Str}),
    inbox: Str -> Seq({from: Str, to: Str, key: Str}),
    requests: Str -> (Str -> Str), ack: Str -> Int, authorizedAck: Str -> Int,
    heartbeats: Str -> Int, authorizedHeartbeats: Str -> Int, heartbeatDone: Set(Str),
    leaseHolder: Str, fence: Int,
    lastLease: {caller: Str, holder: Str, supplied: Int, current: Int, granted: Bool}}; *)
  st,
  \* @type: Int;
  steps
vars == <<st, steps>>

Message == [from : AgentIds, to : AgentIds, key : Keys]
\* @type: ({from: Str, to: Str, key: Str}, Str) => Bool;
MessageFor(message, agent) == message.to = agent
\* @type: Seq({from: Str, to: Str, key: Str}) => Bool;
MessagesWellTyped(messages) ==
  /\ Len(messages) <= MaxMessages
  /\ \A n \in DOMAIN messages : messages[n] \in Message
EmptyLeaseResult == [caller |-> NoId, holder |-> NoId, supplied |-> 0,
                     current |-> 0, granted |-> FALSE]
Init ==
  /\ steps = 0
  /\ st = [
    enrolled |-> {}, active |-> {}, connected |-> {},
    kind |-> [i \in Instances |-> Kind], label |-> [i \in Instances |-> DisplayName],
    id |-> [i \in Instances |-> NoId], firstId |-> [i \in Instances |-> NoId],
    reply |-> [i \in Instances |-> NoId], currentToken |-> [i \in Instances |-> NoToken],
    usedIds |-> {}, issuedOwner |-> [a \in AgentIds |-> NoInstance],
    usedTokens |-> {}, tokenOwner |-> [t \in Tokens |-> NoId],
    tokenInstance |-> [t \in Tokens |-> NoInstance], validTokens |-> {}, revoked |-> {},
    messages |-> <<>>, inbox |-> [a \in AgentIds |-> <<>>],
    requests |-> [a \in AgentIds |-> [k \in Keys |-> NoId]],
    ack |-> [a \in AgentIds |-> 0], authorizedAck |-> [a \in AgentIds |-> 0],
    heartbeats |-> [a \in AgentIds |-> 0], authorizedHeartbeats |-> [a \in AgentIds |-> 0],
    heartbeatDone |-> {}, leaseHolder |-> NoId, fence |-> 0,
    lastLease |-> EmptyLeaseResult]

Resolve(t) == IF t \in st.validTokens THEN st.tokenOwner[t] ELSE NoId
CanCall(i) == i \in st.connected /\ Resolve(st.currentToken[i]) = st.id[i]
FreshIds == AgentIds \ st.usedIds
FreshTokens == Tokens \ st.usedTokens
LiveIds == {st.id[i] : i \in st.active}

(* Initial enrollment and faulty retries share one allocation transition.    *)
Allocate(i, a, t) ==
  LET first == i \notin st.enrolled
  IN st' = [st EXCEPT
    !.enrolled = @ \cup {i}, !.active = @ \cup {i}, !.connected = @ \cup {i},
    !.id[i] = a, !.firstId[i] = IF first THEN a ELSE @, !.reply[i] = a,
    !.currentToken[i] = t, !.usedIds = @ \cup {a}, !.usedTokens = @ \cup {t},
    !.issuedOwner[a] = IF a \in st.usedIds THEN @ ELSE i,
    !.tokenOwner[t] = IF t \in st.usedTokens THEN @ ELSE a,
    !.tokenInstance[t] = IF t \in st.usedTokens THEN @ ELSE i,
    !.validTokens = @ \cup {t}]

Enroll(i) ==
  /\ i \notin st.enrolled
  /\ LET availableIds == IF Fault = "kind-identity" /\ LiveIds # {}
                         THEN LiveIds
                         ELSE IF Fault = "reused-id" THEN AgentIds \ LiveIds ELSE FreshIds
         availableTokens == IF Fault = "shared-token" /\ st.validTokens # {}
                            THEN st.validTokens ELSE FreshTokens
     IN \E a \in availableIds, t \in availableTokens : Allocate(i, a, t)

RetryEnrollment(i) ==
  /\ i \in st.enrolled
  /\ IF Fault = "retry-id" /\ i \in st.active
     THEN \E a \in FreshIds, t \in FreshTokens : Allocate(i, a, t)
     ELSE st' = [st EXCEPT !.reply[i] = st.id[i]]

Disconnect(i) ==
  /\ i \in st.connected
  /\ st' = [st EXCEPT !.connected = @ \ {i}]

Reconnect(i, t) ==
  /\ i \in st.active /\ i \notin st.connected
  /\ t \in st.usedTokens /\ st.tokenInstance[t] = i
  /\ IF Resolve(t) = st.id[i]
     THEN IF Fault = "reconnect-id"
          THEN \E a \in FreshIds :
                 st' = [st EXCEPT !.connected = @ \cup {i}, !.id[i] = a,
                     !.usedIds = @ \cup {a}, !.issuedOwner[a] = i]
          ELSE st' = [st EXCEPT !.connected = @ \cup {i}]
     ELSE UNCHANGED st

Rotate(i) ==
  /\ i \in st.active
  /\ \E t \in FreshTokens :
    LET old == st.currentToken[i]
    IN st' = [st EXCEPT !.currentToken[i] = t, !.usedTokens = @ \cup {t},
        !.tokenOwner[t] = st.id[i], !.tokenInstance[t] = i,
        !.revoked = @ \cup {old},
        !.validTokens = IF Fault = "stale-token" THEN @ \cup {t}
                       ELSE (@ \ {old}) \cup {t}]

Retire(i) ==
  /\ i \in st.active
  /\ st' = [st EXCEPT !.active = @ \ {i}, !.connected = @ \ {i},
      !.revoked = @ \cup {st.currentToken[i]},
      !.validTokens = @ \ {st.currentToken[i]}]

Send(i, j, k) ==
  /\ CanCall(i) /\ j \in st.active /\ Len(st.messages) < MaxMessages
  /\ LET from == Resolve(st.currentToken[i])
         to == st.id[j]
         duplicate == \E n \in DOMAIN st.messages :
           st.messages[n].key = k /\
             (st.messages[n].from = from \/ Fault = "kind-dedupe")
         message == [from |-> from, to |-> to, key |-> k]
         expectedTo == IF st.requests[from][k] = NoId THEN to ELSE st.requests[from][k]
     IN st' = [st EXCEPT
         !.requests[from][k] = expectedTo,
         !.messages = IF duplicate THEN @ ELSE Append(@, message),
         !.inbox = IF duplicate THEN @
                   ELSE IF Fault = "inbox-broadcast"
                        THEN [a \in AgentIds |-> IF a \in LiveIds
                              THEN Append(st.inbox[a], message) ELSE st.inbox[a]]
                        ELSE [st.inbox EXCEPT ![to] = Append(@, message)]]

Ack(i, target) ==
  /\ CanCall(i) /\ target \in LiveIds
  /\ LET caller == Resolve(st.currentToken[i])
         permitted == caller = target
     IN st' = [st EXCEPT
       !.ack[target] = IF permitted \/ Fault = "foreign-ack"
                      THEN Len(st.inbox[target]) ELSE @,
       !.authorizedAck[target] = IF permitted THEN Len(st.inbox[target]) ELSE @]

Heartbeat(i) ==
  /\ CanCall(i) /\ i \notin st.heartbeatDone
  /\ LET caller == Resolve(st.currentToken[i])
     IN st' = [st EXCEPT !.heartbeatDone = @ \cup {i},
       !.authorizedHeartbeats[caller] = @ + 1,
       !.heartbeats = IF Fault = "group-heartbeat"
                     THEN [a \in AgentIds |-> IF a \in LiveIds
                           THEN st.heartbeats[a] + 1 ELSE st.heartbeats[a]]
                     ELSE [st.heartbeats EXCEPT ![caller] = @ + 1]]

Claim(i) ==
  /\ CanCall(i) /\ st.leaseHolder = NoId /\ st.fence < MaxClaims
  /\ st' = [st EXCEPT !.leaseHolder = Resolve(st.currentToken[i]), !.fence = @ + 1]

Complete(i, supplied) ==
  /\ CanCall(i) /\ st.leaseHolder # NoId
  /\ LET caller == Resolve(st.currentToken[i])
         holderOK == caller = st.leaseHolder \/ Fault = "foreign-lease"
         fenceOK == supplied = st.fence \/ Fault = "stale-fence"
         permitted == holderOK /\ fenceOK
     IN st' = [st EXCEPT !.leaseHolder = IF permitted THEN NoId ELSE @,
       !.lastLease = [caller |-> caller, holder |-> st.leaseHolder,
          supplied |-> supplied, current |-> st.fence, granted |-> permitted]]

ExpireLease ==
  /\ st.leaseHolder # NoId
  /\ st' = [st EXCEPT !.leaseHolder = NoId]

EnrollmentNext == \E i \in Instances : Enroll(i)
LifecycleNext == \E i \in Instances : RetryEnrollment(i) \/ Disconnect(i)
                   \/ Rotate(i) \/ Retire(i) \/ (\E t \in Tokens : Reconnect(i, t))
MessagingNext == (\E i, j \in Instances, k \in Keys : Send(i, j, k))
                \/ (\E i \in Instances, a \in AgentIds : Ack(i, a))
PresenceNext == \E i \in Instances : Heartbeat(i)
LeaseNext == (\E i \in Instances : Claim(i)
                  \/ (\E f \in 0..MaxClaims : Complete(i, f))) \/ ExpireLease
NextCore == EnrollmentNext \/
  CASE Scenario = "lifecycle" -> LifecycleNext
    [] Scenario = "messaging" -> MessagingNext
    [] Scenario = "presence" -> PresenceNext
    [] Scenario = "lease" -> LeaseNext
    [] Scenario = "combined" -> LifecycleNext \/ MessagingNext \/ PresenceNext \/ LeaseNext
Next == /\ (Scenario # "combined" \/ steps < MaxTransitions)
        /\ NextCore
        /\ steps' = IF Scenario = "combined" THEN steps + 1 ELSE steps

TypeOK ==
  /\ steps \in 0..MaxTransitions
  /\ (Scenario # "combined" => steps = 0)
  /\ st.enrolled \subseteq Instances /\ st.active \subseteq st.enrolled
  /\ st.connected \subseteq st.active
  /\ st.kind = [i \in Instances |-> Kind] /\ st.label = [i \in Instances |-> DisplayName]
  /\ st.id \in [Instances -> AgentIds \cup {NoId}]
  /\ st.firstId \in [Instances -> AgentIds \cup {NoId}]
  /\ st.reply \in [Instances -> AgentIds \cup {NoId}]
  /\ st.currentToken \in [Instances -> Tokens \cup {NoToken}]
  /\ st.usedIds \subseteq AgentIds /\ st.usedTokens \subseteq Tokens
  /\ st.validTokens \subseteq st.usedTokens /\ st.revoked \subseteq st.usedTokens
  /\ st.issuedOwner \in [AgentIds -> Instances \cup {NoInstance}]
  /\ st.tokenOwner \in [Tokens -> AgentIds \cup {NoId}]
  /\ st.tokenInstance \in [Tokens -> Instances \cup {NoInstance}]
  /\ MessagesWellTyped(st.messages)
  /\ DOMAIN st.inbox = AgentIds
  /\ \A a \in AgentIds : MessagesWellTyped(st.inbox[a])
  /\ st.requests \in [AgentIds -> [Keys -> AgentIds \cup {NoId}]]
  /\ st.ack \in [AgentIds -> 0..MaxMessages]
  /\ st.authorizedAck \in [AgentIds -> 0..MaxMessages]
  /\ st.heartbeats \in [AgentIds -> 0..Cardinality(Instances)]
  /\ st.authorizedHeartbeats \in [AgentIds -> 0..Cardinality(Instances)]
  /\ st.heartbeatDone \subseteq Instances /\ st.leaseHolder \in AgentIds \cup {NoId}
  /\ st.fence \in 0..MaxClaims
  /\ st.lastLease \in [caller : AgentIds \cup {NoId}, holder : AgentIds \cup {NoId},
       supplied : 0..MaxClaims, current : 0..MaxClaims, granted : BOOLEAN]

InstanceIdentityUnique == \A i, j \in st.active : i # j => st.id[i] # st.id[j]
IdentityNotReused == \A i \in st.enrolled : st.issuedOwner[st.id[i]] = i
EnrollmentRetryStable == \A i \in st.enrolled : st.reply[i] = st.firstId[i]
InstanceIdentityStable == \A i \in st.enrolled : st.id[i] = st.firstId[i]
CredentialBoundToInstance ==
  /\ \A i \in st.active : Resolve(st.currentToken[i]) = st.id[i]
  /\ \A t \in st.validTokens :
       \E i \in st.active : st.tokenInstance[t] = i /\ st.tokenOwner[t] = st.firstId[i]
DistinctInstanceCredentials == \A i, j \in st.active :
  i # j => st.currentToken[i] # st.currentToken[j]
RevokedCredentialsRejected == \A t \in st.revoked : Resolve(t) = NoId
InboxAndAckIsolation ==
  /\ \A a \in AgentIds : st.inbox[a] = SelectSeq(st.messages, LAMBDA m : MessageFor(m, a))
  /\ st.ack = st.authorizedAck
IdempotencyScopedToInstance == \A a \in AgentIds, k \in Keys :
  LET matches == {n \in DOMAIN st.messages : st.messages[n].from = a /\ st.messages[n].key = k}
  IN /\ Cardinality(matches) <= 1
     /\ st.requests[a][k] # NoId =>
          (Cardinality(matches) = 1 /\ \A n \in matches : st.messages[n].to = st.requests[a][k])
PresenceAndLeaseIsolation ==
  /\ st.heartbeats = st.authorizedHeartbeats
  /\ st.lastLease.granted => st.lastLease.caller = st.lastLease.holder
LeaseFenceFresh == st.lastLease.granted => st.lastLease.supplied = st.lastLease.current

(* Reachability goals: their violations demonstrate that positive scenarios  *)
(* are exercised. They are not required invariants of the broker.            *)
NoSameKindPeers == ~ (\E i, j \in st.active : i # j /\ st.id[i] # st.id[j]
                       /\ st.currentToken[i] # st.currentToken[j])
NoRetiredThenNew == ~ (\E i \in st.enrolled \ st.active, j \in st.active :
                        st.id[i] # st.id[j])
NoIndependentSameKeySends == ~ (\E i, j \in st.active, k \in Keys :
  i # j /\ st.requests[st.id[i]][k] # NoId /\ st.requests[st.id[j]][k] # NoId)
NoCombinedActivity == ~(Cardinality(st.active) = 2 /\ st.revoked # {} /\
  Len(st.messages) > 0 /\ (\E a \in AgentIds : st.heartbeats[a] > 0) /\ st.leaseHolder # NoId)

Spec == Init /\ [][Next]_vars
===========================================================================
