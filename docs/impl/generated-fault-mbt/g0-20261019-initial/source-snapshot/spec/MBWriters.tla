---------------------------- MODULE MBWriters ----------------------------
(* Root ownership MBT. Start is always an attempted start: rejection is an  *)
(* observable outcome, not a disabled action. Writers use real subprocesses *)
(* in the adapter, including the production startup/recovery and UDS paths. *)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
  \* @type: Set(Str);
  Writers,
  \* @type: Set(Str);
  Roots

VARIABLES
  \* @type: Str -> Str;
  writers,
  \* @type: Str -> Bool;
  rejected,
  \* @type: Str -> Seq(Str);
  records,
  \* @type: Str -> Seq(Int);
  seqs,
  \* @type: Str -> Bool;
  valid,
  \* @type: Str -> Bool;
  torn,
  \* @type: Bool;
  rejectedReadOnly,
  \* @type: {writer: Str, root: Str, socket: Str};
  parameters,
  \* @type: Str;
  action_taken

vars == <<writers, rejected, records, seqs, valid, torn, rejectedReadOnly,
          parameters, action_taken>>
Free(r) == \A w \in Writers : writers[w] # r

Init ==
  /\ writers = [w \in Writers |-> "-"]
  /\ rejected = [w \in Writers |-> FALSE]
  /\ records = [r \in Roots |-> <<>>]
  /\ seqs = [r \in Roots |-> <<>>]
  /\ valid = [r \in Roots |-> TRUE]
  /\ torn = [r \in Roots |-> FALSE]
  /\ rejectedReadOnly = TRUE
  /\ parameters = [writer |-> "-", root |-> "-", socket |-> "-"]
  /\ action_taken = "init"

Start ==
  \E w \in Writers, r \in Roots, s \in {"same", "different"} :
    /\ writers[w] = "-"
    /\ writers' = IF Free(r) THEN [writers EXCEPT ![w] = r] ELSE writers
    /\ rejected' = [rejected EXCEPT ![w] = ~Free(r)]
    /\ torn' = IF Free(r) THEN [torn EXCEPT ![r] = FALSE] ELSE torn
    /\ records' = IF Free(r) /\ torn[r]
                   THEN [records EXCEPT ![r] = Append(@, "recovery")] ELSE records
    /\ seqs' = IF Free(r) /\ torn[r]
                THEN [seqs EXCEPT ![r] = Append(@, Len(records[r]) + 1)] ELSE seqs
    /\ parameters' = [writer |-> w, root |-> r, socket |-> s]
    /\ action_taken' = "start"
    /\ UNCHANGED <<valid, rejectedReadOnly>>

Stop ==
  \E w \in Writers :
    /\ writers[w] # "-"
    /\ writers' = [writers EXCEPT ![w] = "-"]
    /\ parameters' = [writer |-> w, root |-> "-", socket |-> "-"]
    /\ action_taken' = "stop"
    /\ UNCHANGED <<rejected, records, seqs, valid, torn, rejectedReadOnly>>

Send ==
  \E w \in Writers :
    /\ writers[w] # "-"
    /\ LET r == writers[w]
       IN /\ ~torn[r] /\ Len(records[r]) < 2
          /\ records' = [records EXCEPT ![r] = Append(@, w)]
          /\ seqs' = [seqs EXCEPT ![r] = Append(@, Len(records[r]) + 1)]
    /\ parameters' = [writer |-> w, root |-> "-", socket |-> "-"]
    /\ action_taken' = "send"
    /\ UNCHANGED <<writers, rejected, valid, torn, rejectedReadOnly>>

TearTail ==
  \E r \in Roots :
    /\ ~Free(r) /\ ~torn[r] /\ Len(records[r]) > 0
    /\ torn' = [torn EXCEPT ![r] = TRUE]
    /\ parameters' = [writer |-> "-", root |-> r, socket |-> "-"]
    /\ action_taken' = "tear_tail"
    /\ UNCHANGED <<writers, rejected, records, seqs, valid, rejectedReadOnly>>

Next == Start \/ Stop \/ Send \/ TearTail
RootWriterExclusive == \A r \in Roots :
  Cardinality({w \in Writers : writers[w] = r}) <= 1
RejectedStartupReadOnly == rejectedReadOnly
DurableLogsValid == \A r \in Roots : valid[r]

(* Witness goals: Apalache's violated invariant is the requested test trace. *)
NoCompeteW1 == ~(writers["w1"] = "r1" /\ rejected["w2"] /\
  action_taken = "start" /\ parameters.writer = "w2" /\
  parameters.root = "r1" /\ parameters.socket = "different")
NoCompeteW2 == ~(writers["w2"] = "r1" /\ rejected["w1"] /\
  action_taken = "start" /\ parameters.writer = "w1" /\
  parameters.root = "r1" /\ parameters.socket = "different")
NoIndependentRoots == ~(writers["w1"] = "r1" /\ writers["w2"] = "r2" /\
  records["r1"] = <<"w1">> /\ records["r2"] = <<"w2">>)
NoHandoff == ~(writers["w2"] = "r1" /\ records["r1"] = <<"w1", "w2">>)
NoRejectedRecovery == ~(writers["w1"] = "r1" /\ torn["r1"] /\ rejected["w2"] /\
  action_taken = "start" /\ parameters.writer = "w2" /\
  parameters.root = "r1" /\ parameters.socket = "same")

View == <<writers, rejected, records, seqs, torn, parameters, action_taken>>
Spec == Init /\ [][Next]_vars
\* @type: () => Bool;
CInit == /\ Writers = {"w1", "w2"} /\ Roots = {"r1", "r2"}
===========================================================================
