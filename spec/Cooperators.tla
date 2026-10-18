--------------------------- MODULE Cooperators ----------------------------
(***************************************************************************)
(* Cooperators broker — core protocol model.                                *)
(*                                                                          *)
(* Models the safety semantics of docs/design/0001-cooperatorsd-broker.md:  *)
(*   - durable append-only log, per-record sequence position                *)
(*   - direct messaging: idempotent send, peek cursors, volatile acks       *)
(*   - queues: claim leases with fencing, expiry, fail, dead-letter         *)
(*   - documents: publish, CAS update, exclusive edit leases                *)
(*   - crash: volatile state lost, durable log preserved                    *)
(*                                                                          *)
(* Modeling assumptions (documented limits):                                *)
(*   A1 Durability: append+fsync is atomic (design §3 "append → fsync →     *)
(*      ack"), so an acknowledged record is never lost. Torn-tail recovery  *)
(*      drops only a never-acknowledged suffix; it is therefore not a       *)
(*      distinct action here.                                               *)
(*   A2 Single writer: the daemon is the only log writer, so every action   *)
(*      is atomic. Lease renew is omitted: it only extends a deadline, and  *)
(*      expiry is already nondeterministic (TTL abstraction).               *)
(*   A3 Rejections (VERSION_CONFLICT, CONFLICT_CLAIMED, LEASE_LOST,         *)
(*      deduped resend) are action-disabling events: they leave no trace    *)
(*      in the log. The invariants below assert exactly that no invalid     *)
(*      record ever appears.                                                *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Agents,          \* agent identities
          Keys,            \* idempotency keys (per sender)
          MaxAttempts,     \* queue delivery cap before dead-letter
          Q,               \* the single modeled queue resource
          D                \* the single modeled document resource

VARIABLES log,             \* durable log: sequence of records; fence = position
          ackCursor        \* volatile: ackCursor[a] = log position acked by a

vars == <<log, ackCursor>>

Res == {Q, D}

MsgR    == [type : {"msg"}, from : Agents, to : Agents, key : Keys]
ClaimR  == [type : {"claim"}, res : Res, holder : Agents]
TermR   == [type : {"release", "expire", "complete", "fail"}, res : Res, fence : Nat]
DocR    == [type : {"docput"}, doc : {D}, base : Nat, holder : Agents, fence : Nat]
Records == MsgR \cup ClaimR \cup TermR \cup DocR

TypeOK ==
  /\ log \in Seq(Records)
  /\ ackCursor \in [Agents -> Nat]

(***************************** derived state ******************************)
IdxOf(res, types) == {i \in DOMAIN log : log[i].type \in types /\ log[i].res = res}
MaxOf(S)          == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : x >= y

UsedPairs == {<<log[i].from, log[i].key>> :
                i \in {j \in DOMAIN log : log[j].type = "msg"}}

MaxClaimBefore(res, p) == MaxOf({j \in DOMAIN log :
                                   log[j].type = "claim" /\ j <= p /\ log[j].res = res})
Terminated(res, f) ==
  f > 0 /\ \E i \in DOMAIN log :
             log[i].type \in {"release", "expire", "complete", "fail"}
             /\ i > f /\ log[i].res = res /\ log[i].fence = f
ActiveFence(res) ==
  LET f == MaxClaimBefore(res, Len(log))
  IN  IF f = 0 \/ Terminated(res, f) THEN 0 ELSE f
ActiveHolder(res) == log[ActiveFence(res)].holder

AttemptsQ == Cardinality(IdxOf(Q, {"fail", "expire"}))
Dead      == AttemptsQ >= MaxAttempts
Completed == IdxOf(Q, {"complete"}) # {}

DocPuts  == SelectSeq(log, LAMBDA r : r.type = "docput" /\ r.doc = D)
Version  == Len(DocPuts)

MaxMsgSeqTo(a) == MaxOf({i \in DOMAIN log : log[i].type = "msg" /\ log[i].to = a})

(******************************* actions **********************************)
Send(from, to, key) ==
  /\ <<from, key>> \notin UsedPairs          \* else dedupe: no append (A3)
  /\ log' = Append(log, [type |-> "msg", from |-> from, to |-> to, key |-> key])
  /\ UNCHANGED ackCursor

Ack(a) ==
  \E c \in (ackCursor[a] + 1)..MaxMsgSeqTo(a) :
    ackCursor' = [ackCursor EXCEPT ![a] = c]   \* monotone, volatile
  /\ UNCHANGED log

Claim(res, holder) ==
  /\ ActiveFence(res) = 0                       \* else CONFLICT_CLAIMED
  /\ res = Q => (~Completed /\ ~Dead)           \* queue: claimable task
  /\ log' = Append(log, [type |-> "claim", res |-> res, holder |-> holder])
  /\ UNCHANGED ackCursor

Expire(res) ==                                  \* TTL abstraction (A2)
  /\ ActiveFence(res) # 0
  /\ log' = Append(log, [type |-> "expire", res |-> res, fence |-> ActiveFence(res)])
  /\ UNCHANGED ackCursor

Complete(h) ==
  /\ ActiveFence(Q) # 0 /\ ActiveHolder(Q) = h
  /\ log' = Append(log, [type |-> "complete", res |-> Q, fence |-> ActiveFence(Q)])
  /\ UNCHANGED ackCursor

Fail(h) ==
  /\ ActiveFence(Q) # 0 /\ ActiveHolder(Q) = h
  /\ log' = Append(log, [type |-> "fail", res |-> Q, fence |-> ActiveFence(Q)])
  /\ UNCHANGED ackCursor

ReleaseD(h) ==
  /\ ActiveFence(D) # 0 /\ ActiveHolder(D) = h
  /\ log' = Append(log, [type |-> "release", res |-> D, fence |-> ActiveFence(D)])
  /\ UNCHANGED ackCursor

Publish(h) ==
  /\ Version = 0                                \* else EXISTS
  /\ log' = Append(log, [type |-> "docput", doc |-> D, base |-> 0,
                         holder |-> h, fence |-> 0])
  /\ UNCHANGED ackCursor

Update(h) ==
  LET f == ActiveFence(D)
  IN  /\ Version >= 1                           \* else NOT_FOUND
      /\ f # 0 => ActiveHolder(D) = h           \* else CONFLICT_CLAIMED
      /\ log' = Append(log, [type |-> "docput", doc |-> D, base |-> Version,
                             holder |-> h, fence |-> f])
      /\ UNCHANGED ackCursor

Crash ==                                        \* A1: log survives intact
  /\ \E a \in Agents : ackCursor[a] # 0
  /\ ackCursor' = [a \in Agents |-> 0]
  /\ UNCHANGED log

Next ==
  \/ \E f, t \in Agents, k \in Keys : Send(f, t, k)
  \/ \E a \in Agents : Ack(a)
  \/ \E h \in Agents : Claim(Q, h) \/ Claim(D, h) \/ Complete(h)
                       \/ Fail(h) \/ ReleaseD(h) \/ Publish(h) \/ Update(h)
  \/ Expire(Q) \/ Expire(D)
  \/ Crash

ClaimTask     == \E h \in Agents : Claim(Q, h)
TerminateTask == \E h \in Agents : Complete(h) \/ Fail(h) \/ (ActiveFence(Q) # 0 /\ Expire(Q))

Init == log = <<>> /\ ackCursor = [a \in Agents |-> 0]

Spec == Init /\ [][Next]_vars
       /\ WF_vars(Next) /\ WF_vars(ClaimTask) /\ WF_vars(TerminateTask)

(***************************** invariants *********************************)
NoDuplicateMessages ==
  \A i, j \in DOMAIN log :
    (i # j /\ log[i].type = "msg" /\ log[j].type = "msg")
      => (log[i].from # log[j].from \/ log[i].key # log[j].key)

ClaimSeparation ==   \* mutual exclusion: one live claim per resource
  \A i, j \in DOMAIN log :
    (i < j /\ log[i].type = "claim" /\ log[j].type = "claim"
             /\ log[i].res = log[j].res)
      => \E k \in (i + 1)..(j - 1) :
           log[k].type \in {"release", "expire", "complete", "fail"}
           /\ log[k].res = log[i].res /\ log[k].fence = i

FenceFreshness ==    \* no stale lease ever mutates a resource (LEASE_LOST)
  \A i \in DOMAIN log :
    log[i].type \in {"release", "expire", "complete", "fail"}
      => log[i].fence # 0
         /\ log[i].fence = MaxClaimBefore(log[i].res, i - 1)

AttemptsBound == AttemptsQ <= MaxAttempts

DeadMeansNoActiveLease == Dead => ActiveFence(Q) = 0

DocVersionChain ==   \* CAS: versions are 0-based contiguous
  \A k \in 1..Version : DocPuts[k].base = k - 1

DocHolderRespected ==
  \A i \in DOMAIN log :
    (log[i].type = "docput" /\ log[i].fence # 0)
      => log[log[i].fence].holder = log[i].holder

DocWriteFenceLive == \* a claimed write's lease was never terminated before it
  \A i \in DOMAIN log :
    (log[i].type = "docput" /\ log[i].fence # 0)
      => ~ \E k \in (log[i].fence + 1)..(i - 1) :
             log[k].type \in {"release", "expire"}
             /\ log[k].res = D /\ log[k].fence = log[i].fence

(****************************** liveness **********************************)
TaskResolved == <>(Completed \/ Dead)

Sym == Permutations(Agents)

StateConstraint == Len(log) <= 6
===========================================================================
