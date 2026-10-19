------------------------------ MODULE MBFaults ------------------------------
(* Implementation-facing failure/recovery contract. Every attempt has a     *)
(* named action, explicit inputs, response, and an observation projection.   *)
(* Ghost receipts/monitors are NOT observations of disk or broker state.     *)
(* Profiles share a trace format, not an unproved cross-profile composition. *)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS Scenario, Fault, MaxSteps
Scenarios == {"writer", "storage", "inbox", "deadletter", "document",
              "status", "rotation", "identity"}
Faults == {"none", "writer", "prelock-replay", "unowned-write",
  "continue-after-io", "ack-before-sync", "lose-ack", "wrong-receipt", "expired-cursor",
  "drop-projection", "duplicate-projection", "doc-file-gate", "early-publish",
  "tail-window", "status-mutates", "no-rotation", "last-segment", "rotation-loss",
  "kind-identity", "reused-id", "retry-id", "reconnect-id", "shared-token", "stale-token",
  "stale-enrollment"}
ASSUME /\ Scenario \in Scenarios /\ Fault \in Faults /\ MaxSteps \in Nat

VARIABLES s, steps, action_taken, parameters, result, observation, enrollmentHistory
vars == <<s, steps, action_taken, parameters, result, observation, enrollmentHistory>>
Writers == {"p1", "p2"}
Launches == {"launch1", "launch2"}
AgentIds == {"id1", "id2", "id3"}
Tokens == {"token1", "token2", "token3"}
Capacity == 2
MaxRecords == 3
RotateAt == 2
TailWindow == 2
Sizes == {1, 3}
IdFault == IF Fault \in {"kind-identity", "reused-id", "retry-id", "reconnect-id", "shared-token", "stale-token"}
          THEN Fault ELSE "none"
Registry == INSTANCE AgentInstances WITH Instances <- Launches, AgentIds <- AgentIds,
  Tokens <- Tokens, Keys <- {"shared-key"}, Kind <- "codex", DisplayName <- "codex",
  Scenario <- "lifecycle", Fault <- IdFault, st <- s, steps <- 0

Ids(xs) == {xs[i].id : i \in DOMAIN xs}
SeqSet(xs) == {xs[i] : i \in DOMAIN xs}
MaxOf(xs) == CHOOSE n \in xs : \A m \in xs : n >= m
ValidLength(xs) == MaxOf({0} \cup {n \in 1..Len(xs) :
                           \A i \in 1..n : xs[i].seq = i})
Prefix(xs) == SubSeq(xs, 1, ValidLength(xs))
RECURSIVE Flatten(_)
Flatten(xs) == IF xs = <<>> THEN <<>> ELSE Head(xs) \o Flatten(Tail(xs))
RECURSIVE Sum(_)
Sum(xs) == IF xs = <<>> THEN 0 ELSE Head(xs) + Sum(Tail(xs))

(* Observation is a state expression, never a variable constrained to agree  *)
(* with the model. An implementation adapter must obtain its counterpart    *)
(* independently. Opaque IDs/tokens may be renamed only through a bijection  *)
(* built from actual enrollment replies; duplicate concrete IDs must fail.  *)
Observation == CASE
  Scenario = "writer" -> [owners |-> s.owners, records |-> s.records,
                           torn |-> s.torn, rejected |-> s.rejected]
  [] Scenario = "storage" -> [records |-> s.disk, phase |-> s.phase,
                              recoveryRequired |-> s.poisoned, receipts |-> s.receipts]
  [] Scenario = "inbox" -> [head |-> s.sent, ack |-> s.ack, cursor |-> s.cursor,
                            messages |-> s.delivered, pending |-> s.sent - s.ack]
  [] Scenario = "deadletter" -> [terminal |-> s.terminal, deadRecords |-> s.records, up |-> s.up]
  [] Scenario = "document" -> [file |-> s.file, version |-> s.version, up |-> s.up]
  [] Scenario = "status" -> [records |-> s.records, reported |-> s.reported]
  [] Scenario = "rotation" -> [records |-> s.records, files |-> s.files,
          cursor |-> s.cursor, readBack |-> s.readBack, up |-> s.up]
  [] Scenario = "identity" -> [ids |-> s.id, replies |-> s.reply,
          currentTokens |-> s.currentToken, kinds |-> s.kind, labels |-> s.label,
          active |-> s.active, connected |-> s.connected]


(* The launch key is the initial credential; only its hash is persisted by
   the implementation. This ghost remembers history to check retry policy,
   and is deliberately excluded from Observation. *)
RetryAllowed(i) == i \in s.active
                  /\ s.currentToken[i] = enrollmentHistory.firstToken[i]
Stamp(op, actor, value, mode, response) ==
  /\ steps' = steps + 1 /\ action_taken' = op /\ result' = response
  /\ parameters' = [scenario |-> Scenario, actor |-> actor, value |-> value, mode |-> mode]
  /\ observation' = Observation'
  /\ enrollmentHistory' = IF Scenario = "identity" THEN
       [enrollmentHistory EXCEPT
        !.firstToken = IF op = "enroll" /\ actor \notin s.enrolled
                       THEN [@ EXCEPT ![actor] = s'.currentToken[actor]] ELSE @,
        !.retried = IF op = "enroll" /\ actor \in s.enrolled /\ response = "ok"
                    THEN @ \cup {actor} ELSE @,
        !.rejected = IF op = "enroll" /\ response = "conflict"
                     THEN @ \cup {actor} ELSE @,
        !.retired = IF op = "retire" THEN @ \cup {actor} ELSE @,
        !.freshAfterRetire = @ \/ (op = "enroll" /\ actor \notin s.enrolled
                                   /\ enrollmentHistory.retired # {}),
        !.safe = IF op = "enroll" /\ actor \in s.enrolled
                 THEN @ /\ (response = IF RetryAllowed(actor) THEN "ok" ELSE "conflict")
                 ELSE @]
     ELSE enrollmentHistory

WriterInit == s = [owners |-> {}, records |-> <<>>, torn |-> FALSE, tears |-> 0,
  writeOwned |-> TRUE, replayOwned |-> TRUE, rejectedReadOnly |-> TRUE,
  rejected |-> {}, stopped |-> "-"]
Start(p, mode) ==
  /\ p \notin s.owners
  /\ LET busy == s.owners # {}
         admitted == ~busy \/ (Fault = "writer" /\ mode = "different")
         repair == (~busy \/ Fault = "prelock-replay") /\ s.torn
         owners == IF admitted THEN s.owners \cup {p} ELSE s.owners
         records == IF repair THEN Append(s.records, 99) ELSE s.records
     IN /\ s' = [s EXCEPT !.owners = owners, !.records = records,
          !.torn = IF repair THEN FALSE ELSE @,
          !.rejected = IF admitted THEN @ \ {p} ELSE @ \cup {p},
          !.replayOwned = @ /\ (repair => p \in owners),
          !.rejectedReadOnly = @ /\ (~admitted => records = s.records /\ ~repair)]
        /\ Stamp("start", p, 0, mode, IF admitted THEN "ok" ELSE "busy")
Stop(p) ==
  /\ p \in s.owners
  /\ s' = [s EXCEPT !.owners = @ \ {p}, !.stopped = p]
  /\ Stamp("stop", p, 0, "-", "ok")
WriterAppend(p) ==
  /\ Len(s.records) < MaxRecords /\ ~s.torn
  /\ LET applied == p \in s.owners \/ Fault = "unowned-write"
     IN /\ s' = [s EXCEPT !.records = IF applied THEN Append(@, Len(@) + 1) ELSE @,
                       !.writeOwned = @ /\ (applied => p \in s.owners)]
        /\ Stamp("writer-append", p, 0, "-", IF applied THEN "ok" ELSE "not-owner")
TearTail ==
  /\ s.owners # {} /\ s.records # <<>> /\ s.tears = 0
  /\ s' = [s EXCEPT !.torn = TRUE, !.tears = 1]
  /\ Stamp("tear-tail", "-", 0, "-", "ok")
WriterNext == (\E p \in Writers : Stop(p) \/ WriterAppend(p)
              \/ (\E mode \in {"same", "different"} : Start(p, mode))) \/ TearTail

StorageInit == s = [disk |-> <<>>, durable |-> <<>>, receipts |-> <<>>,
  nextSeq |-> 1, nextId |-> 1, pending |-> 0, phase |-> "ready", poisoned |-> FALSE,
  failures |-> 0, partialFailure |-> FALSE, recovered |-> FALSE, committedAfterRecovery |-> FALSE,
  rejected |-> FALSE]
AppendBytes(mode) ==
  /\ s.phase = "ready" /\ s.nextId <= MaxRecords
  /\ mode = "whole" \/ s.failures = 0
  /\ IF s.poisoned
     THEN /\ s' = [s EXCEPT !.rejected = TRUE]
          /\ Stamp("append-bytes", "-", s.nextId, mode, "recovery-required")
     ELSE LET partial == mode = "partial"
              early == Fault = "ack-before-sync" /\ ~partial
          IN /\ s' = [s EXCEPT
                !.disk = Append(@, [seq |-> IF partial THEN 0 ELSE s.nextSeq, id |-> s.nextId]),
                !.nextId = @ + 1, !.pending = IF partial THEN 0 ELSE s.nextId,
                !.phase = IF partial THEN "ready" ELSE "written",
                !.poisoned = partial /\ Fault # "continue-after-io",
                !.failures = IF partial THEN 1 ELSE @,
                !.partialFailure = @ \/ partial,
                !.recovered = IF partial THEN FALSE ELSE @,
                !.committedAfterRecovery = IF partial THEN FALSE ELSE @,
                !.receipts = IF early THEN Append(@, [id |-> s.nextId, seq |-> s.nextSeq]) ELSE @]
             /\ Stamp("append-bytes", "-", s.nextId, mode,
                      IF partial THEN "io-error" ELSE IF early THEN "ok" ELSE "pending")
SyncOK ==
  /\ s.phase = "written"
  /\ s' = [s EXCEPT !.durable = s.disk,
      !.receipts = Append(@, [id |-> s.pending,
                       seq |-> IF Fault = "wrong-receipt" THEN s.nextSeq + 1 ELSE s.nextSeq]),
      !.nextSeq = @ + 1, !.pending = 0, !.phase = "ready",
      !.committedAfterRecovery = @ \/ (s.recovered /\ s.failures > 0)]
  /\ Stamp("sync-ok", "-", 0, "-", "ok")
SyncFail ==
  /\ s.phase = "written" /\ s.failures = 0
  /\ s' = [s EXCEPT !.poisoned = (Fault # "continue-after-io"),
      !.pending = 0, !.phase = "ready", !.failures = 1,
      !.recovered = FALSE, !.committedAfterRecovery = FALSE]
  /\ Stamp("sync-fail", "-", 0, "-", "io-error")
RecoverStorage(kept) ==
  /\ kept \in Len(s.durable)..Len(s.disk)
  /\ LET retained == Prefix(SubSeq(s.disk, 1, kept))
         repaired == IF Fault = "lose-ack" /\ retained # <<>>
                     THEN SubSeq(retained, 1, Len(retained) - 1) ELSE retained
     IN s' = [s EXCEPT !.disk = repaired, !.durable = repaired,
        !.nextSeq = Len(repaired) + 1, !.pending = 0, !.phase = "ready",
        !.poisoned = FALSE, !.recovered = TRUE]
  /\ Stamp("recover-storage", "-", kept, "retained-prefix", "ok")
InspectStorage == /\ UNCHANGED s /\ Stamp("inspect-storage", "-", 0, "-", "ok")
StorageNext == (\E mode \in {"whole", "partial"} : AppendBytes(mode))
  \/ SyncOK \/ SyncFail \/ (\E kept \in 0..MaxRecords : RecoverStorage(kept)) \/ InspectStorage

InboxInit == s = [sent |-> 0, ack |-> 0, expired |-> {}, cursor |-> 0,
  delivered |-> <<>>, waited |-> FALSE, blocked |-> FALSE]
Pending == s.sent - s.ack
InboxSend ==
  /\ s.sent < MaxRecords
  /\ LET full == Pending >= Capacity
     IN /\ s' = [s EXCEPT !.sent = IF full THEN @ ELSE @ + 1,
                     !.blocked = @ \/ full, !.waited = FALSE]
        /\ Stamp("inbox-send", "-", 0, "expiring", IF full THEN "full" ELSE "ok")
ExpireInbox ==
  /\ s.sent > s.ack
  /\ s' = [s EXCEPT !.expired = @ \cup (1..s.sent), !.waited = FALSE]
  /\ Stamp("expire-inbox", "-", 0, "controlled-clock", "ok")
InboxWait ==
  LET live == SelectSeq([n \in 1..s.sent |-> n], LAMBDA n : n > s.ack /\ n \notin s.expired)
      batch == IF Len(live) = 0 THEN <<>> ELSE <<Head(live)>>
      scanned == IF batch = <<>> THEN s.sent ELSE Head(batch)
      cursor == IF Fault = "expired-cursor" /\ batch = <<>> THEN s.ack ELSE scanned
  IN /\ s' = [s EXCEPT !.cursor = cursor, !.delivered = batch, !.waited = TRUE]
     /\ Stamp("inbox-wait", "-", 0, "batch-one", "ok")
InboxAck ==
  /\ s.waited
  /\ s' = [s EXCEPT !.ack = s.cursor, !.waited = FALSE]
  /\ Stamp("inbox-ack", "-", s.cursor, "returned-cursor", "ok")
InboxNext == InboxSend \/ ExpireInbox \/ InboxWait \/ InboxAck

DeadInit == s = [terminal |-> FALSE, pending |-> FALSE, records |-> <<>>,
  phase |-> "idle", up |-> TRUE, crashes |-> 0, failures |-> 0,
  crashedAfterAppend |-> FALSE, recoveredMissing |-> FALSE]
MarkDead ==
  /\ s.up /\ ~s.terminal
  /\ s' = [s EXCEPT !.terminal = TRUE, !.pending = TRUE]
  /\ Stamp("mark-dead", "-", 1, "durable-terminal", "pending")
ProjectDead ==
  /\ s.up /\ s.terminal /\ s.phase = "idle" /\ Len(s.records) < 2
  /\ LET available == s.pending \/ s.records # <<>>
         append == available /\ (s.records = <<>> \/ Fault = "duplicate-projection")
     IN /\ s' = [s EXCEPT !.records = IF append THEN Append(@, 1) ELSE @,
                            !.phase = IF available THEN "appended" ELSE "idle"]
        /\ Stamp("project-dead", "-", 1, "dedupe-by-terminal",
                 IF ~available THEN "no-work" ELSE IF append THEN "written" ELSE "already-projected")
FinishProjection ==
  /\ s.up /\ s.phase = "appended"
  /\ s' = [s EXCEPT !.pending = FALSE, !.phase = "idle"]
  /\ Stamp("finish-projection", "-", 0, "-", "ok")
FailProjection ==
  /\ s.up /\ s.terminal /\ s.records = <<>> /\ s.failures = 0
  /\ s' = [s EXCEPT !.failures = 1]
  /\ Stamp("fail-projection", "-", 0, "before-dead-append", "io-error")
CrashDead ==
  /\ s.up /\ s.crashes = 0
  /\ s' = [s EXCEPT !.up = FALSE, !.crashes = 1, !.phase = "idle",
                       !.crashedAfterAppend = (s.phase = "appended")]
  /\ Stamp("crash-dead", "-", 0, "-", "stopped")
RecoverDead ==
  /\ ~s.up
  /\ s' = [s EXCEPT !.up = TRUE,
       !.recoveredMissing = s.terminal /\ s.records = <<>>,
       !.pending = s.terminal /\ s.records = <<>> /\ Fault # "drop-projection"]
  /\ Stamp("recover-dead", "-", 0, "reconcile-terminal-records", "ok")
DeadNext == MarkDead \/ ProjectDead \/ FinishProjection \/ FailProjection \/ CrashDead \/ RecoverDead

DocInit == s = [file |-> FALSE, version |-> 0, phase |-> "idle", up |-> TRUE,
  failures |-> 0, crashes |-> 0, interrupted |-> FALSE]
BeginPublish ==
  /\ s.up /\ s.phase = "idle"
  /\ LET exists == s.version > 0 \/ (Fault = "doc-file-gate" /\ s.file)
         early == Fault = "early-publish" /\ ~exists
     IN /\ s' = [s EXCEPT !.file = IF exists THEN @ ELSE TRUE,
          !.phase = IF exists THEN @ ELSE "preparing"]
        /\ Stamp("begin-publish", "-", 0, "same-name",
                 IF exists THEN "exists" ELSE IF early THEN "ok" ELSE "pending")
CommitPublish ==
  /\ s.up /\ s.phase = "preparing"
  /\ s' = [s EXCEPT !.version = 1, !.phase = "idle"]
  /\ Stamp("commit-publish", "-", 0, "-", "ok")
AbortPublish ==
  /\ s.up /\ s.phase = "preparing" /\ s.failures = 0
  /\ s' = [s EXCEPT !.phase = "idle", !.failures = 1]
  /\ Stamp("abort-publish", "-", 0, "after-file-before-record", "io-error")
CrashDoc ==
  /\ s.up /\ s.crashes = 0
  /\ s' = [s EXCEPT !.up = FALSE, !.phase = "idle", !.crashes = 1,
                       !.interrupted = (s.phase = "preparing" /\ s.version = 0)]
  /\ Stamp("crash-doc", "-", 0, "-", "stopped")
RecoverDoc == /\ ~s.up /\ s' = [s EXCEPT !.up = TRUE]
              /\ Stamp("recover-doc", "-", 0, "-", "ok")
ReadDoc == /\ s.up /\ UNCHANGED s
           /\ Stamp("read-doc", "-", 0, "-", IF s.version = 0 THEN "not-found" ELSE "ok")
DocNext == BeginPublish \/ CommitPublish \/ AbortPublish \/ CrashDoc \/ RecoverDoc \/ ReadDoc

StatusInit == s = [records |-> <<>>, reported |-> -1, sampled |-> FALSE, readOnly |-> TRUE]
AppendStatus(size) ==
  /\ Len(s.records) < MaxRecords
  /\ s' = [s EXCEPT !.records = Append(@, [id |-> Len(@) + 1, bytes |-> size]), !.sampled = FALSE]
  /\ Stamp("append-status", "-", size, "encoded-record-units", "ok")
SampleStatus ==
  /\ LET oversized == s.records # <<>> /\ s.records[Len(s.records)].bytes > TailWindow
         mutate == Fault = "status-mutates" /\ s.records # <<>>
     IN /\ s' = [s EXCEPT !.sampled = TRUE,
          !.reported = IF Fault = "tail-window" /\ oversized THEN -1 ELSE Len(s.records),
          !.records = IF mutate THEN SubSeq(@, 1, Len(@) - 1) ELSE @,
          !.readOnly = @ /\ (~mutate)]
        /\ Stamp("sample-status", "-", 0, "read-only", "ok")
StatusNext == (\E size \in Sizes : AppendStatus(size)) \/ SampleStatus

RotationInit == s = [records |-> <<>>, files |-> << <<>> >>, pendingSize |-> 0,
  phase |-> "idle", cursor |-> [file |-> 1, seq |-> 0], readBack |-> <<>>,
  checked |-> FALSE, up |-> TRUE, crashes |-> 0, interrupted |-> FALSE, resumed |-> FALSE]
FileBytes(file) == Sum([n \in 1..Len(file) |-> s.records[file[n]].bytes])
BeginRotateAppend(size) ==
  /\ s.up /\ s.phase = "idle" /\ Len(s.records) < MaxRecords
  /\ s' = [s EXCEPT !.pendingSize = size, !.phase = "preparing", !.checked = FALSE]
  /\ Stamp("begin-rotating-append", "-", size, "encoded-record-units", "pending")
PrepareSegment ==
  /\ s.up /\ s.phase = "preparing"
  /\ LET active == Len(s.files)
         rotate == s.files[active] # <<>> /\
                   FileBytes(s.files[active]) + s.pendingSize > RotateAt /\ Fault # "no-rotation"
         files == IF ~rotate THEN s.files ELSE IF Fault = "rotation-loss"
                  THEN << <<>> >> ELSE Append(s.files, <<>>)
     IN s' = [s EXCEPT !.files = files, !.phase = "prepared"]
  /\ Stamp("prepare-segment", "-", 0, "before-record-commit", "pending")
CommitRotatingAppend ==
  /\ s.up /\ s.phase = "prepared"
  /\ LET id == Len(s.records) + 1
     IN s' = [s EXCEPT !.records = Append(@, [id |-> id, bytes |-> s.pendingSize]),
         !.files[Len(s.files)] = Append(@, id), !.phase = "idle", !.pendingSize = 0,
         !.resumed = @ \/ s.crashes > 0]
  /\ Stamp("commit-rotating-append", "-", 0, "-", "ok")
SaveCursor ==
  /\ s.up /\ s.phase = "idle"
  /\ LET seq == Len(s.records)
         file == IF seq = 0 THEN 1 ELSE CHOOSE n \in DOMAIN s.files : seq \in SeqSet(s.files[n])
     IN s' = [s EXCEPT !.cursor = [file |-> file, seq |-> seq], !.checked = FALSE]
  /\ Stamp("save-cursor", "-", 0, "logical-sequence", "ok")
ReadCursor ==
  /\ s.up /\ s.phase = "idle"
  /\ LET source == IF Fault = "last-segment" THEN s.files[Len(s.files)] ELSE Flatten(s.files)
     IN s' = [s EXCEPT !.readBack = SelectSeq(source, LAMBDA id : id > s.cursor.seq), !.checked = TRUE]
  /\ Stamp("read-cursor", "-", s.cursor.seq, "all-segments", "ok")
CrashRotation ==
  /\ s.up /\ s.crashes = 0
  /\ s' = [s EXCEPT !.up = FALSE, !.phase = "idle", !.pendingSize = 0, !.crashes = 1,
       !.interrupted = (s.phase = "prepared" /\ Len(s.files) > 1
                        /\ s.files[Len(s.files)] = <<>>)]
  /\ Stamp("crash-rotation", "-", 0, "prepared-segment-may-be-empty", "stopped")
RecoverRotation == /\ ~s.up /\ s' = [s EXCEPT !.up = TRUE]
                   /\ Stamp("recover-rotation", "-", 0, "keep-segment-order", "ok")
RotationNext == (\E size \in Sizes : BeginRotateAppend(size)) \/ PrepareSegment
  \/ CommitRotatingAppend \/ SaveCursor \/ ReadCursor \/ CrashRotation \/ RecoverRotation

IdentityInit == Registry!Init
Enroll(i) ==
  /\ IF i \notin s.enrolled
     THEN /\ Registry!Enroll(i) /\ Stamp("enroll", i, 0, "fresh-launch-key", "ok")
     ELSE IF RetryAllowed(i) \/ Fault = "stale-enrollment"
          THEN /\ Registry!RetryEnrollment(i) /\ Stamp("enroll", i, 0, "fresh-launch-key", "ok")
          ELSE /\ s' = [s EXCEPT !.reply[i] = s.id[i]]
               /\ Stamp("enroll", i, 0, "fresh-launch-key", "conflict")
Disconnect(i) == /\ Registry!Disconnect(i) /\ Stamp("disconnect", i, 0, "-", "ok")
Reconnect(i) ==
  /\ Registry!Reconnect(i, s.currentToken[i])
  /\ Stamp("reconnect", i, 0, "current-token", "ok")
RotateToken(i) == /\ Registry!Rotate(i) /\ Stamp("rotate-token", i, 0, "-", "ok")
Retire(i) == /\ Registry!Retire(i) /\ Stamp("retire", i, 0, "-", "ok")
Authenticate(i, mode) ==
  /\ i \in s.enrolled
  /\ LET tokens == IF mode = "current" THEN {s.currentToken[i]}
                   ELSE {t \in s.revoked : s.tokenInstance[t] = i}
     IN \E t \in tokens :
          /\ UNCHANGED s
          /\ Stamp("authenticate", i, 0, mode,
                 IF Registry!Resolve(t) = Registry!NoId THEN "unauthenticated" ELSE "ok")
IdentityNext == \E i \in Launches : Enroll(i) \/ Disconnect(i) \/ Reconnect(i)
  \/ RotateToken(i) \/ Retire(i) \/ (\E mode \in {"current", "revoked"} : Authenticate(i, mode))

Init == /\ steps = 0 /\ action_taken = "init" /\ result = "none"
  /\ enrollmentHistory = [firstToken |-> [i \in Launches |-> Registry!NoToken],
       retried |-> {}, rejected |-> {}, retired |-> {}, freshAfterRetire |-> FALSE, safe |-> TRUE]
  /\ parameters = [scenario |-> Scenario, actor |-> "-", value |-> 0, mode |-> "-"]
  /\ (CASE Scenario = "writer" -> WriterInit [] Scenario = "storage" -> StorageInit
       [] Scenario = "inbox" -> InboxInit [] Scenario = "deadletter" -> DeadInit
       [] Scenario = "document" -> DocInit [] Scenario = "status" -> StatusInit
       [] Scenario = "rotation" -> RotationInit [] Scenario = "identity" -> IdentityInit)
  /\ observation = Observation
Next == /\ steps < MaxSteps
  /\ CASE Scenario = "writer" -> WriterNext [] Scenario = "storage" -> StorageNext
       [] Scenario = "inbox" -> InboxNext [] Scenario = "deadletter" -> DeadNext
       [] Scenario = "document" -> DocNext [] Scenario = "status" -> StatusNext
       [] Scenario = "rotation" -> RotationNext [] Scenario = "identity" -> IdentityNext
Spec == Init /\ [][Next]_vars


WriterSafe == IF Scenario = "writer" THEN /\ Cardinality(s.owners) <= 1
  /\ s.writeOwned /\ s.replayOwned /\ s.rejectedReadOnly ELSE TRUE
ObservationFaithful == observation = Observation
ReceiptsSurvive == IF Scenario = "storage" THEN Ids(s.receipts) \subseteq Ids(s.disk) ELSE TRUE
ReceiptSequenceMatches == IF Scenario = "storage" THEN
  \A n \in DOMAIN s.receipts : s.receipts[n] \in SeqSet(s.disk) ELSE TRUE
StorageSequenceValid == IF Scenario = "storage"
                       THEN ValidLength(s.durable) = Len(s.durable) ELSE TRUE
SyncBeforeReceipt == IF Scenario = "storage" THEN
  \A n \in DOMAIN s.receipts : s.receipts[n] \in SeqSet(s.durable) ELSE TRUE
UncertainWriterQuarantined == IF Scenario = "storage"
  THEN (s.failures > 0 /\ ~s.recovered) => (s.poisoned /\ s.phase = "ready") ELSE TRUE
InboxSafe == IF Scenario = "inbox" THEN
  /\ Pending \in 0..Capacity /\ s.ack <= s.cursor /\ s.cursor <= s.sent
  /\ s.waited => ((s.ack + 1)..s.cursor \ s.expired) \subseteq SeqSet(s.delivered)
  /\ (s.waited /\ s.sent > s.ack /\ (s.ack + 1)..s.sent \subseteq s.expired)
       => s.cursor > s.ack
  ELSE TRUE
DeadLetterRecoverable == IF Scenario = "deadletter"
                         THEN s.terminal => (s.pending \/ s.records # <<>>) ELSE TRUE
DeadLetterUnique == IF Scenario = "deadletter"
                    THEN Len(s.records) <= 1 /\ (s.records # <<>> => s.terminal) ELSE TRUE
DocumentRetryable == IF Scenario = "document" THEN
  (action_taken = "begin-publish" /\ s.version = 0 => result # "exists") ELSE TRUE
PublishReceiptSafe == IF Scenario = "document" THEN
  (action_taken \in {"begin-publish", "commit-publish"} /\ result = "ok")
    => (s.file /\ s.version = 1) ELSE TRUE
StatusAccurate == IF Scenario = "status" THEN s.sampled => s.reported = Len(s.records) ELSE TRUE
StatusReadOnly == IF Scenario = "status" THEN s.readOnly ELSE TRUE
RotationContents == IF Scenario = "rotation"
                    THEN Flatten(s.files) = [n \in 1..Len(s.records) |-> s.records[n].id] ELSE TRUE
RotationBounded == IF Scenario = "rotation" THEN \A file \in SeqSet(s.files) :
  FileBytes(file) <= RotateAt \/ (Len(file) = 1 /\ s.records[Head(file)].bytes > RotateAt)
  ELSE TRUE
RotationCursorSafe == IF Scenario = "rotation" THEN
  /\ (s.cursor.seq > 0 => s.cursor.seq \in SeqSet(s.files[s.cursor.file]))
  /\ (s.checked => s.readBack = SubSeq(Flatten(s.files), s.cursor.seq + 1, Len(s.records)))
  ELSE TRUE
IdentitySafe == IF Scenario = "identity" THEN
  /\ Registry!InstanceIdentityUnique /\ Registry!IdentityNotReused
  /\ Registry!EnrollmentRetryStable /\ Registry!InstanceIdentityStable
  /\ Registry!CredentialBoundToInstance /\ Registry!DistinctInstanceCredentials
  /\ Registry!RevokedCredentialsRejected /\ enrollmentHistory.safe ELSE TRUE

TypeOK == /\ steps \in 0..MaxSteps /\ action_taken \in STRING /\ result \in STRING
  /\ enrollmentHistory \in [firstToken : [Launches -> Tokens \cup {Registry!NoToken}],
       retried : SUBSET Launches, rejected : SUBSET Launches, retired : SUBSET Launches,
       freshAfterRetire : BOOLEAN, safe : BOOLEAN]
  /\ parameters \in [scenario : Scenarios, actor : Writers \cup Launches \cup {"-"},
                       value : 0..99, mode : STRING]
  /\ CASE Scenario = "writer" ->
       s \in [owners : SUBSET Writers, records : Seq(1..99), torn : BOOLEAN, tears : 0..1,
         writeOwned : BOOLEAN, replayOwned : BOOLEAN, rejectedReadOnly : BOOLEAN,
         rejected : SUBSET Writers, stopped : Writers \cup {"-"}]
    [] Scenario = "storage" ->
       /\ s.disk \in Seq([seq : 0..4, id : 1..3]) /\ Len(s.disk) <= MaxRecords
       /\ s.durable \in Seq([seq : 0..4, id : 1..3])
       /\ s.receipts \in Seq([id : 1..3, seq : 1..4])
       /\ s.nextSeq \in 1..4 /\ s.nextId \in 1..4
       /\ s.pending \in 0..3 /\ s.phase \in {"ready", "written"}
       /\ s.poisoned \in BOOLEAN /\ s.failures \in 0..1 /\ s.recovered \in BOOLEAN
       /\ s.partialFailure \in BOOLEAN
       /\ s.committedAfterRecovery \in BOOLEAN
       /\ s.rejected \in BOOLEAN
    [] Scenario = "inbox" ->
       s \in [sent : 0..3, ack : 0..3, expired : SUBSET (1..3), cursor : 0..3,
         delivered : Seq(1..3), waited : BOOLEAN, blocked : BOOLEAN]
    [] Scenario = "deadletter" ->
       s \in [terminal : BOOLEAN, pending : BOOLEAN, records : Seq({1}),
         phase : {"idle", "appended"}, up : BOOLEAN, crashes : 0..1, failures : 0..1,
         crashedAfterAppend : BOOLEAN, recoveredMissing : BOOLEAN]
    [] Scenario = "document" ->
       s \in [file : BOOLEAN, version : 0..1, phase : {"idle", "preparing"},
         up : BOOLEAN, failures : 0..1, crashes : 0..1, interrupted : BOOLEAN]
    [] Scenario = "status" ->
       /\ s.records \in Seq([id : 1..3, bytes : Sizes])
       /\ s.reported \in (-1)..3 /\ s.sampled \in BOOLEAN /\ s.readOnly \in BOOLEAN
    [] Scenario = "rotation" ->
       /\ s.records \in Seq([id : 1..3, bytes : Sizes])
       /\ s.files \in Seq(Seq(1..3)) /\ Len(s.files) >= 1
       /\ s.pendingSize \in Sizes \cup {0} /\ s.phase \in {"idle", "preparing", "prepared"}
       /\ s.cursor \in [file : 1..4, seq : 0..3] /\ s.readBack \in Seq(1..3)
       /\ s.checked \in BOOLEAN /\ s.up \in BOOLEAN /\ s.crashes \in 0..1
       /\ s.interrupted \in BOOLEAN /\ s.resumed \in BOOLEAN
    [] Scenario = "identity" -> Registry!TypeOK

(* Negative reachability predicates: violations yield correct-policy tests. *)
NoWriterRejection == ~(Scenario = "writer" /\ result = "busy" /\ parameters.mode = "different")
NoRejectedRepair == ~(Scenario = "writer" /\ result = "busy" /\ s.torn /\ parameters.mode = "same")
NoWriterHandoff == ~(Scenario = "writer" /\ s.stopped # "-" /\ parameters.actor # s.stopped /\ result = "ok"
                     /\ action_taken = "start" /\ s.records # <<>>)
NoUnownedWrite == ~(Scenario = "writer" /\ s.owners # {} /\ action_taken = "writer-append"
  /\ parameters.actor \notin s.owners /\ result = "not-owner")
NoStorageRecovery == ~(Scenario = "storage" /\ s.failures = 1 /\ ~s.partialFailure /\ s.rejected
  /\ s.recovered /\ s.committedAfterRecovery /\ s.receipts # <<>> /\ action_taken = "inspect-storage")
NoPartialRecovery == ~(Scenario = "storage" /\ s.partialFailure /\ s.recovered
  /\ s.committedAfterRecovery /\ s.receipts # <<>> /\ action_taken = "inspect-storage"
  /\ s.nextId = 3 /\ s.rejected)
NoAcknowledgedRecovery == ~(Scenario = "storage" /\ s.failures = 1 /\ s.recovered
  /\ s.committedAfterRecovery /\ Len(s.receipts) = 2 /\ action_taken = "inspect-storage")
NoInboxCapacityRecovery == ~(Scenario = "inbox" /\ s.blocked /\ {1, 2} \subseteq s.expired
  /\ s.delivered = <<>> /\ s.sent = 3 /\ s.ack = 2
  /\ action_taken = "inbox-send" /\ result = "ok")
NoProjectionRecovery == ~(Scenario = "deadletter" /\ s.failures = 1 /\ s.crashes = 1
  /\ s.recoveredMissing /\ s.up /\ s.records = <<1>> /\ ~s.pending /\ action_taken = "finish-projection")
NoProjectionCrashRetry == ~(Scenario = "deadletter" /\ s.crashedAfterAppend
  /\ s.up /\ s.records = <<1>> /\ action_taken = "project-dead" /\ result = "already-projected")
NoDocumentRetry == ~(Scenario = "document" /\ s.failures = 1 /\ s.version = 1
  /\ action_taken = "read-doc" /\ result = "ok")
NoDocumentCrashRetry == ~(Scenario = "document" /\ s.interrupted /\ s.version = 1
  /\ s.up /\ action_taken = "read-doc" /\ result = "ok")
NoLargeStatus == ~(Scenario = "status" /\ s.records # <<>>
  /\ s.records[Len(s.records)].bytes > TailWindow /\ s.sampled /\ s.reported = Len(s.records))
NoRotatedCursor == ~(Scenario = "rotation" /\ s.checked /\ s.cursor.seq = 1
  /\ s.readBack = <<2, 3>> /\ Len(s.files) >= 2)
NoRotationCrashRecovery == ~(Scenario = "rotation" /\ s.interrupted /\ s.resumed /\ s.up
  /\ Len(s.files) >= 2 /\ s.records # <<>> /\ s.checked /\ s.readBack # <<>>)
NoOversizedRotation == ~(Scenario = "rotation" /\ Len(s.records) = 2 /\ Len(s.files) = 2
  /\ s.records[1].bytes > RotateAt /\ s.checked /\ s.readBack = <<1, 2>>)
NoSameKindEnrollment == ~(Scenario = "identity" /\ Cardinality(s.active) = 2
  /\ action_taken = "enroll" /\ Registry!InstanceIdentityUnique /\ Registry!DistinctInstanceCredentials)
NoRevokedAuthentication == ~(Scenario = "identity" /\ Cardinality(s.active) = 2
  /\ action_taken = "authenticate" /\ parameters.mode = "revoked" /\ result = "unauthenticated")
NoStableReconnect == ~(Scenario = "identity" /\ Cardinality(s.active) = 2
  /\ action_taken = "reconnect" /\ Registry!InstanceIdentityStable)
NoEnrollmentRetry == ~(Scenario = "identity" /\ Cardinality(s.active) = 2
  /\ action_taken = "enroll" /\ parameters.actor \in enrollmentHistory.retried
  /\ result = "ok" /\ Registry!EnrollmentRetryStable)
NoStaleEnrollmentRetry == ~(Scenario = "identity" /\ Cardinality(s.active) = 2
  /\ action_taken = "enroll" /\ parameters.actor \in s.active /\ result = "conflict")
NoRetiredEnrollmentRetry == ~(Scenario = "identity" /\ action_taken = "enroll"
  /\ parameters.actor \in enrollmentHistory.retired /\ result = "conflict")
NoRetiredThenFresh == ~(Scenario = "identity" /\ enrollmentHistory.freshAfterRetire
  /\ Cardinality(s.enrolled) = 2 /\ Cardinality(s.active) = 1
  /\ action_taken = "enroll" /\ Registry!IdentityNotReused)
NoRetiredAuthentication == ~(Scenario = "identity" /\ enrollmentHistory.freshAfterRetire
  /\ parameters.actor \in enrollmentHistory.retired /\ action_taken = "authenticate"
  /\ parameters.mode = "current" /\ result = "unauthenticated")
===========================================================================
