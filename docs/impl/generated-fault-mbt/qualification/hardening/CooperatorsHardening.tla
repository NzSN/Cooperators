----------------------- MODULE CooperatorsHardening -----------------------
(***************************************************************************)
(* Implementation obligations omitted by the atomic protocol models.        *)
(* Each Scenario is checked separately: these are small requirement models,  *)
(* not a refinement proof of the Rust implementation or their composition.   *)
(* Fault = "none" selects the proposed behavior. Other values deliberately   *)
(* reproduce reviewed failure mechanisms; the checker must reject them.      *)
(* Storage acknowledgements are historical ghost state and survive crashes.  *)
(* pendingProjection is a DURABLE recovery obligation (possibly derived from *)
(* queue terminal events), not an additional required production state file. *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS Scenario, Fault

Scenarios == {"writer", "storage", "inbox", "deadletter", "document",
              "status", "rotation"}
Faults == Scenarios \cup {"none", "replay", "cursor"}
ASSUME /\ Scenario \in Scenarios
       /\ Fault \in Faults

VARIABLE st
vars == <<st>>

Writers == {"p1", "p2"}
MaxWrites == 2
Capacity == 2
BatchLimit == 1
MaxRecords == 3
RotateAt == 2
TailWindow == 2

SeqSet(xs) == {xs[i] : i \in DOMAIN xs}
Ids(xs) == {xs[i].id : i \in DOMAIN xs}
MaxOf(S) == CHOOSE x \in S : \A y \in S : x >= y
Min2(a, b) == IF a <= b THEN a ELSE b
ValidLength(xs) == MaxOf({0} \cup {n \in 1..Len(xs) :
                                      \A i \in 1..n : xs[i].seq = i})
ReplayPrefix(xs) == SubSeq(xs, 1, ValidLength(xs))
RECURSIVE Flatten(_)
Flatten(files) == IF files = <<>> THEN <<>>
                  ELSE Head(files) \o Flatten(Tail(files))

WriterInit == st = [owners |-> {}, replaying |-> {}]
Acquire(p) ==
  /\ p \notin st.owners
  /\ st.owners = {} \/ Fault = "writer"
  /\ st' = [st EXCEPT !.owners = @ \cup {p}]
BeginReplay(p) ==
  /\ p \notin st.replaying
  /\ p \in st.owners \/ Fault = "replay"
  /\ st' = [st EXCEPT !.replaying = @ \cup {p}]
Release(p) ==
  /\ p \in st.owners
  /\ st' = [st EXCEPT !.owners = @ \ {p}, !.replaying = @ \ {p}]
WriterNext == \E p \in Writers : Acquire(p) \/ BeginReplay(p) \/ Release(p)

(* disk may contain writes whose fsync failed; durable is the fsynced prefix. *)
StorageInit == st = [disk |-> <<>>, durable |-> <<>>, nextSeq |-> 1,
                     nextId |-> 1, phase |-> "ready", pending |-> 0,
                     acknowledged |-> {}, poisoned |-> FALSE]
WriteRecord ==
  /\ st.phase = "ready" /\ ~st.poisoned /\ st.nextId <= MaxWrites
  /\ st' = [st EXCEPT
      !.disk = Append(@, [seq |-> st.nextSeq, id |-> st.nextId]),
      !.pending = st.nextId, !.nextId = @ + 1, !.phase = "written"]
SyncSuccess ==
  /\ st.phase = "written"
  /\ st' = [st EXCEPT !.durable = st.disk,
      !.acknowledged = @ \cup {st.pending}, !.nextSeq = @ + 1,
      !.pending = 0, !.phase = "ready"]
SyncFailure ==
  /\ st.phase = "written"
  /\ st' = [st EXCEPT !.poisoned = (Fault # "storage"),
                           !.pending = 0, !.phase = "ready"]
RecoverStorage ==
  \E kept \in Len(st.durable)..Len(st.disk) :
    LET repaired == ReplayPrefix(SubSeq(st.disk, 1, kept))
    IN st' = [st EXCEPT !.disk = repaired, !.durable = repaired,
        !.nextSeq = Len(repaired) + 1, !.phase = "ready",
        !.pending = 0, !.poisoned = FALSE]
StorageNext == WriteRecord \/ SyncSuccess \/ SyncFailure \/ RecoverStorage

InboxInit == st = [sent |-> 0, ack |-> 0, expired |-> {}, cursor |-> 0,
                   scanned |-> 0, delivered |-> {}, waited |-> FALSE]
PendingCount == st.sent - st.ack
LiveAfterAck == SelectSeq([i \in 1..st.sent |-> i],
                          LAMBDA i : i > st.ack /\ i \notin st.expired)
AllPendingExpired == (st.ack + 1)..st.sent \subseteq st.expired
InboxSend ==
  /\ ~st.waited /\ PendingCount < Capacity /\ st.sent < MaxRecords
  /\ st' = [st EXCEPT !.sent = @ + 1]
ExpireMessage ==
  /\ ~st.waited
  /\ \E i \in (1..st.sent) \ st.expired :
       st' = [st EXCEPT !.expired = @ \cup {i}]
InboxWait ==
  /\ ~st.waited
  /\ LET batch == SubSeq(LiveAfterAck, 1, Min2(Len(LiveAfterAck), BatchLimit))
         end == IF Len(batch) = BatchLimit THEN batch[Len(batch)] ELSE st.sent
         returned == IF Fault = "inbox"
                     THEN IF batch = <<>> THEN st.ack ELSE batch[Len(batch)]
                     ELSE end
     IN st' = [st EXCEPT !.cursor = returned, !.scanned = end,
                         !.delivered = SeqSet(batch), !.waited = TRUE]
InboxAck ==
  /\ st.waited
  /\ st' = [st EXCEPT !.ack = st.cursor, !.waited = FALSE]
InboxNext == InboxSend \/ ExpireMessage \/ InboxWait \/ InboxAck

DeadInit == st = [terminal |-> FALSE, published |-> FALSE,
                  pendingProjection |-> FALSE, todo |-> FALSE,
                  up |-> TRUE, crashes |-> 0, failures |-> 0]
MarkDead ==
  /\ st.up /\ ~st.terminal
  /\ st' = [st EXCEPT !.terminal = TRUE, !.todo = TRUE,
                          !.pendingProjection = (Fault # "deadletter")]
ProjectDeadLetter ==
  /\ st.up /\ st.todo /\ st.terminal /\ ~st.published
  /\ st' = [st EXCEPT !.published = TRUE, !.pendingProjection = FALSE,
                          !.todo = FALSE]
ProjectionFailure ==
  /\ st.up /\ st.todo /\ st.failures = 0
  /\ st' = [st EXCEPT !.todo = FALSE, !.failures = 1]
RetryProjection ==
  /\ st.up /\ st.pendingProjection /\ ~st.todo /\ ~st.published
  /\ st' = [st EXCEPT !.todo = TRUE]
CrashDead ==
  /\ st.up /\ st.crashes = 0
  /\ st' = [st EXCEPT !.up = FALSE, !.todo = FALSE, !.crashes = 1]
RecoverDead ==
  /\ ~st.up
  /\ st' = [st EXCEPT !.up = TRUE, !.todo = st.pendingProjection]
DeadNext == MarkDead \/ ProjectDeadLetter \/ ProjectionFailure \/ RetryProjection
            \/ CrashDead \/ RecoverDead

DocInit == st = [fileExists |-> FALSE, version |-> 0, phase |-> "idle",
                 result |-> "none"]
CanPublish == st.phase = "idle" /\ st.version = 0
              /\ (Fault # "document" \/ ~st.fileExists)
BeginPublish ==
  /\ CanPublish
  /\ st' = [st EXCEPT !.fileExists = TRUE, !.phase = "preparing",
                          !.result = "none"]
CommitPublish ==
  /\ st.phase = "preparing"
  /\ st' = [st EXCEPT !.version = 1, !.phase = "idle", !.result = "ok"]
AbortPublish ==
  /\ st.phase = "preparing"
  (* Covers blob/append failure and crash before the initial durable record. *)
  /\ st' = [st EXCEPT !.phase = "idle", !.result = "error"]
DocNext == BeginPublish \/ CommitPublish \/ AbortPublish

StatusInit == st = [head |-> 0, lastRecordBytes |-> 0,
                    reported |-> -1, sampled |-> FALSE]
AppendForStatus ==
  /\ ~st.sampled /\ st.head < MaxRecords
  /\ \E size \in {1, TailWindow + 1} :
       st' = [st EXCEPT !.head = @ + 1, !.lastRecordBytes = size]
SampleStatus ==
  /\ ~st.sampled
  /\ st' = [st EXCEPT !.sampled = TRUE,
      !.reported = IF Fault = "status" /\ st.lastRecordBytes > TailWindow
                   THEN -1 ELSE st.head]
StatusNext == AppendForStatus \/ SampleStatus

(* Rotation uses fixed-size records; RotateAt abstracts the byte threshold. *)
RotationInit == st = [records |-> <<>>, files |-> << <<>> >>,
                      cursor |-> [file |-> 1, seq |-> 0],
                      readBack |-> <<>>, checked |-> FALSE]
AppendRotating ==
  /\ Len(st.records) < MaxRecords
  /\ LET id == Len(st.records) + 1
         active == Len(st.files)
         rotate == Len(st.files[active]) >= RotateAt /\ Fault # "rotation"
         files == IF rotate THEN Append(st.files, <<id>>)
                  ELSE [st.files EXCEPT ![active] = Append(@, id)]
     IN st' = [st EXCEPT !.records = Append(@, id), !.files = files,
                         !.checked = FALSE]
SaveCursor ==
  st' = [st EXCEPT !.cursor = [file |-> Len(st.files), seq |-> Len(st.records)],
                   !.checked = FALSE]
ReadFromCursor ==
  LET source == IF Fault = "cursor" THEN st.files[Len(st.files)]
                ELSE Flatten(st.files)
  IN st' = [st EXCEPT !.readBack = SelectSeq(source, LAMBDA i : i > st.cursor.seq),
                      !.checked = TRUE]
RotationNext == AppendRotating \/ SaveCursor \/ ReadFromCursor

Init == CASE Scenario = "writer" -> WriterInit
          [] Scenario = "storage" -> StorageInit
          [] Scenario = "inbox" -> InboxInit
          [] Scenario = "deadletter" -> DeadInit
          [] Scenario = "document" -> DocInit
          [] Scenario = "status" -> StatusInit
          [] Scenario = "rotation" -> RotationInit
Next == CASE Scenario = "writer" -> WriterNext
          [] Scenario = "storage" -> StorageNext
          [] Scenario = "inbox" -> InboxNext
          [] Scenario = "deadletter" -> DeadNext
          [] Scenario = "document" -> DocNext
          [] Scenario = "status" -> StatusNext
          [] Scenario = "rotation" -> RotationNext

TypeOK == CASE Scenario = "writer" ->
                 st \in [owners : SUBSET Writers, replaying : SUBSET Writers]
          [] Scenario = "storage" ->
                 /\ st.disk \in Seq([seq : 1..(MaxWrites + 1), id : 1..MaxWrites])
                 /\ st.durable \in Seq([seq : 1..(MaxWrites + 1), id : 1..MaxWrites])
                 /\ st.nextSeq \in 1..(MaxWrites + 1)
                 /\ st.nextId \in 1..(MaxWrites + 1)
                 /\ st.pending \in 0..MaxWrites
                 /\ st.phase \in {"ready", "written"}
                 /\ st.acknowledged \subseteq 1..MaxWrites
                 /\ st.poisoned \in BOOLEAN
          [] Scenario = "inbox" ->
                 st \in [sent : 0..MaxRecords, ack : 0..MaxRecords,
                         expired : SUBSET (1..MaxRecords), cursor : 0..MaxRecords,
                         scanned : 0..MaxRecords, delivered : SUBSET (1..MaxRecords),
                         waited : BOOLEAN]
          [] Scenario = "deadletter" ->
                 st \in [terminal : BOOLEAN, published : BOOLEAN,
                         pendingProjection : BOOLEAN, todo : BOOLEAN, up : BOOLEAN,
                         crashes : 0..1, failures : 0..1]
          [] Scenario = "document" ->
                 st \in [fileExists : BOOLEAN, version : 0..1,
                         phase : {"idle", "preparing"}, result : {"none", "ok", "error"}]
          [] Scenario = "status" ->
                 st \in [head : 0..MaxRecords, lastRecordBytes : {0, 1, TailWindow + 1},
                         reported : (-1)..MaxRecords, sampled : BOOLEAN]
          [] Scenario = "rotation" ->
                 /\ st.records \in Seq(1..MaxRecords)
                 /\ st.files \in Seq(Seq(1..MaxRecords)) /\ Len(st.files) >= 1
                 /\ st.cursor \in [file : 1..MaxRecords, seq : 0..MaxRecords]
                 /\ st.readBack \in Seq(1..MaxRecords) /\ st.checked \in BOOLEAN

RootWriterExclusive == IF Scenario = "writer"
                       THEN Cardinality(st.owners) <= 1 ELSE TRUE
ReplayRequiresOwnership == IF Scenario = "writer"
                          THEN st.replaying \subseteq st.owners ELSE TRUE
AcknowledgedRecordsSurvive == IF Scenario = "storage"
                             THEN st.acknowledged \subseteq Ids(st.durable) ELSE TRUE
DurableSequenceValid == IF Scenario = "storage"
                       THEN ValidLength(st.durable) = Len(st.durable) ELSE TRUE
InboxAccounting == IF Scenario = "inbox"
                   THEN 0 <= PendingCount /\ PendingCount <= Capacity ELSE TRUE
CursorCoversScannedPrefix == IF Scenario = "inbox"
                           THEN st.waited => st.cursor = st.scanned ELSE TRUE
CursorDoesNotSkipLive == IF Scenario = "inbox"
                        THEN st.waited =>
                          ((st.ack + 1)..st.cursor \ st.expired) \subseteq st.delivered
                        ELSE TRUE
ExpiredOnlyCursorProgress == IF Scenario = "inbox"
                            THEN (st.waited /\ PendingCount > 0 /\ AllPendingExpired)
                                 => st.cursor > st.ack ELSE TRUE
DeadLetterAccountedFor == IF Scenario = "deadletter"
                         THEN st.terminal => (st.published \/ st.pendingProjection)
                         ELSE TRUE
DeadLetterHasTerminalSource == IF Scenario = "deadletter"
                              THEN st.published => st.terminal ELSE TRUE
UnpublishedDocRetryable == IF Scenario = "document"
                          THEN (st.version = 0 /\ st.phase = "idle") => CanPublish
                          ELSE TRUE
SuccessfulPublishHasVersion == IF Scenario = "document"
                               THEN st.result = "ok" => (st.version = 1 /\ st.fileExists)
                               ELSE TRUE
StatusHeadMatchesDurableLog == IF Scenario = "status"
                              THEN st.sampled => st.reported = st.head ELSE TRUE
RotationBounded == IF Scenario = "rotation"
                   THEN \A i \in DOMAIN st.files : Len(st.files[i]) <= RotateAt ELSE TRUE
RotationPreservesRecords == IF Scenario = "rotation"
                           THEN Flatten(st.files) = st.records ELSE TRUE
CursorAnchorPreserved == IF Scenario = "rotation"
                        THEN st.cursor.seq = 0 \/
                             (st.cursor.file \in DOMAIN st.files /\
                              st.cursor.seq \in SeqSet(st.files[st.cursor.file])) ELSE TRUE
CursorReadContinuity == IF Scenario = "rotation"
                       THEN st.checked =>
                         st.readBack = SubSeq(st.records, st.cursor.seq + 1, Len(st.records))
                       ELSE TRUE

(* Fairness is explicit: at most one crash and one failed projection are     *)
(* modeled; after these finite faults the recovery worker must get CPU time. *)
FairInboxWait == IF Scenario = "inbox" THEN InboxWait ELSE FALSE
FairInboxAck == IF Scenario = "inbox" THEN InboxAck ELSE FALSE
FairRecoverDead == IF Scenario = "deadletter" THEN RecoverDead ELSE FALSE
FairRetryProjection == IF Scenario = "deadletter" THEN RetryProjection ELSE FALSE
FairProjectDeadLetter == IF Scenario = "deadletter" THEN ProjectDeadLetter ELSE FALSE
Fairness == /\ WF_vars(FairInboxWait) /\ WF_vars(FairInboxAck)
            /\ WF_vars(FairRecoverDead) /\ WF_vars(FairRetryProjection)
            /\ WF_vars(FairProjectDeadLetter)
ExpiredCapacityEventuallyFreed ==
  (IF Scenario = "inbox" THEN PendingCount = Capacity /\ AllPendingExpired ELSE FALSE)
    ~> (IF Scenario = "inbox" THEN PendingCount < Capacity ELSE TRUE)
DeadLetterEventuallyPublished ==
  (IF Scenario = "deadletter" THEN st.terminal ELSE FALSE)
    ~> (IF Scenario = "deadletter" THEN st.published ELSE TRUE)

Spec == Init /\ [][Next]_vars /\ Fairness
===========================================================================
