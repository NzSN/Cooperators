-------------------------- MODULE MBCooperators ---------------------------
(***************************************************************************)
(* MBT variant of Cooperators.tla, instrumented for ModelMirrors replay:   *)
(*   - concrete string constants (Apalache), flattened log records         *)
(*   - action_taken / parameters drive the SUT adapter                     *)
(*   - ackCursor counts messages per agent (== impl inbox seq)             *)
(*   - TraceComplete violation yields witness traces for Apalache          *)
(* Oracle boundary (see spec/mbt/README.md): traces exercise enabled-      *)
(* action equivalence and full abstract state evolution; rejection paths   *)
(* are covered by the TLC-checked Cooperators.tla, not by replay.          *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
  \* @type: Set(Str);
  Agents,
  \* @type: Set(Str);
  Keys,
  \* @type: Int;
  MaxAttempts,
  \* @type: Str;
  Q,
  \* @type: Str;
  D

VARIABLES
  \* @type: Seq({type: Str, from: Str, to: Str, key: Str, res: Str, holder: Str, fence: Int, base: Int});
  log,
  \* @type: Str -> Int;
  ackCursor,
  \* @type: {from: Str, to: Str, key: Str, res: Str, holder: Str, cursor: Int};
  parameters,
  \* @type: Str;
  action_taken

vars == <<log, ackCursor, parameters, action_taken>>

\* @type: Set(Int) => Int;
MaxOf(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : x >= y

\* @type: Str => Int;
MsgCountTo(a) == Cardinality({i \in DOMAIN log : log[i].type = "msg" /\ log[i].to = a})

\* @type: () => Set(<<Str, Str>>);
UsedPairs == {<<log[i].from, log[i].key>> :
                i \in {j \in DOMAIN log : log[j].type = "msg"}}

\* @type: Str => Int;
ActiveFence(res) ==
  LET f == MaxOf({j \in DOMAIN log : log[j].type = "claim" /\ log[j].res = res})
  IN  IF f = 0 \/ \E i \in DOMAIN log :
                    log[i].type \in {"release", "expire", "complete", "fail"}
                    /\ i > f /\ log[i].res = res /\ log[i].fence = f
      THEN 0 ELSE f

\* @type: Str => Str;
ActiveHolder(res) == log[ActiveFence(res)].holder

AttemptsQ == Cardinality({i \in DOMAIN log : log[i].type \in {"fail", "expire"}
                                             /\ log[i].res = Q})
\* Dead after the MaxAttempts-th failed delivery (claim) attempt: the
\* implementation increments its attempt counter at claim time and
\* dead-letters when a terminal event finds attempts >= max_attempts.
Dead      == AttemptsQ >= MaxAttempts
Completed == {i \in DOMAIN log : log[i].type = "complete" /\ log[i].res = Q} # {}
Version   == Cardinality({i \in DOMAIN log : log[i].type = "docput"})

Init ==
  /\ log = <<>>
  /\ ackCursor = [a \in Agents |-> 0]
  /\ parameters = [from |-> "-", to |-> "-", key |-> "-",
                   res |-> "-", holder |-> "-", cursor |-> -1]
  /\ action_taken = "init"

Send ==
  \E f \in Agents :
    \E t \in Agents :
      \E k \in Keys :
        /\ <<f, k>> \notin UsedPairs
        /\ log' = Append(log, [type |-> "msg", from |-> f, to |-> t, key |-> k,
                               res |-> "-", holder |-> "-", fence |-> 0, base |-> 0])
        /\ parameters' = [from |-> f, to |-> t, key |-> k,
                          res |-> "-", holder |-> "-", cursor |-> -1]
        /\ action_taken' = "send"
        /\ UNCHANGED ackCursor

Ack ==
  \E a \in Agents :
    \E c \in (ackCursor[a] + 1)..MsgCountTo(a) :
      /\ ackCursor' = [ackCursor EXCEPT ![a] = c]
      /\ parameters' = [from |-> "-", to |-> "-", key |-> "-",
                        res |-> "-", holder |-> a, cursor |-> c]
      /\ action_taken' = "ack"
      /\ UNCHANGED log

Claim ==
  \E res \in {Q, D} :
    \E h \in Agents :
      /\ ActiveFence(res) = 0
      /\ res = Q => (~Completed /\ ~Dead)
      /\ res = D => Version >= 1
      /\ log' = Append(log, [type |-> "claim", from |-> "-", to |-> "-", key |-> "-",
                             res |-> res, holder |-> h, fence |-> 0, base |-> 0])
      /\ parameters' = [from |-> "-", to |-> "-", key |-> "-",
                        res |-> res, holder |-> h, cursor |-> -1]
      /\ action_taken' = "claim"
      /\ UNCHANGED ackCursor

Expire ==
  \E res \in {Q, D} :
    /\ ActiveFence(res) # 0
    /\ log' = Append(log, [type |-> "expire", from |-> "-", to |-> "-", key |-> "-",
                           res |-> res, holder |-> "-",
                           fence |-> ActiveFence(res), base |-> 0])
    /\ parameters' = [from |-> "-", to |-> "-", key |-> "-",
                      res |-> res, holder |-> "-", cursor |-> -1]
    /\ action_taken' = "expire"
    /\ UNCHANGED ackCursor

Complete ==
  \E h \in Agents :
    /\ ActiveFence(Q) # 0 /\ ActiveHolder(Q) = h
    /\ log' = Append(log, [type |-> "complete", from |-> "-", to |-> "-", key |-> "-",
                           res |-> Q, holder |-> h,
                           fence |-> ActiveFence(Q), base |-> 0])
    /\ parameters' = [from |-> "-", to |-> "-", key |-> "-",
                      res |-> Q, holder |-> h, cursor |-> -1]
    /\ action_taken' = "complete"
    /\ UNCHANGED ackCursor

Fail ==
  \E h \in Agents :
    /\ ActiveFence(Q) # 0 /\ ActiveHolder(Q) = h
    /\ log' = Append(log, [type |-> "fail", from |-> "-", to |-> "-", key |-> "-",
                           res |-> Q, holder |-> h,
                           fence |-> ActiveFence(Q), base |-> 0])
    /\ parameters' = [from |-> "-", to |-> "-", key |-> "-",
                      res |-> Q, holder |-> h, cursor |-> -1]
    /\ action_taken' = "fail"
    /\ UNCHANGED ackCursor

Release ==
  \E h \in Agents :
    /\ ActiveFence(D) # 0 /\ ActiveHolder(D) = h
    /\ log' = Append(log, [type |-> "release", from |-> "-", to |-> "-", key |-> "-",
                           res |-> D, holder |-> h,
                           fence |-> ActiveFence(D), base |-> 0])
    /\ parameters' = [from |-> "-", to |-> "-", key |-> "-",
                      res |-> D, holder |-> h, cursor |-> -1]
    /\ action_taken' = "release"
    /\ UNCHANGED ackCursor

Publish ==
  \E h \in Agents :
    /\ Version = 0
    /\ log' = Append(log, [type |-> "docput", from |-> "-", to |-> "-", key |-> "-",
                           res |-> D, holder |-> h, fence |-> 0, base |-> 0])
    /\ parameters' = [from |-> "-", to |-> "-", key |-> "-",
                      res |-> D, holder |-> h, cursor |-> -1]
    /\ action_taken' = "publish"
    /\ UNCHANGED ackCursor

Update ==
  \E h \in Agents :
    /\ Version >= 1
    /\ ActiveFence(D) # 0 => ActiveHolder(D) = h
    /\ log' = Append(log, [type |-> "docput", from |-> "-", to |-> "-", key |-> "-",
                           res |-> D, holder |-> h,
                           fence |-> ActiveFence(D), base |-> Version])
    /\ parameters' = [from |-> "-", to |-> "-", key |-> "-",
                      res |-> D, holder |-> h, cursor |-> -1]
    /\ action_taken' = "update"
    /\ UNCHANGED ackCursor

Crash ==
  /\ \E a \in Agents : ackCursor[a] # 0
  /\ ackCursor' = [a \in Agents |-> 0]
  /\ parameters' = [from |-> "-", to |-> "-", key |-> "-",
                    res |-> "-", holder |-> "-", cursor |-> -1]
  /\ action_taken' = "crash"
  /\ UNCHANGED log

Next == Send \/ Ack \/ Claim \/ Expire \/ Complete \/ Fail
        \/ Release \/ Publish \/ Update \/ Crash

View == [log |-> log, ackCursor |-> ackCursor]

\* Apalache treats a violated invariant as a counterexample = the test trace.
\* TraceComplete gives the base battery; the No* invariants are action-
\* focused witnesses that force ack/crash/release/expire/complete/dead
\* coverage (those actions never appear in the shortest Len-bounded traces
\* because they do not grow the log).
TraceComplete == Len(log) < 6
NoAck == action_taken /= "ack"
NoCrash == action_taken /= "crash"
NoRelease == action_taken /= "release"
NoExpire == action_taken /= "expire"
NoComplete == action_taken /= "complete"
NoDead == ~Dead
NoFail == action_taken /= "fail"

Spec == Init /\ [][Next]_vars

\* @type: () => Bool;
CInit ==
  /\ Agents = {"a1", "a2"}
  /\ Keys = {"k1", "k2"}
  /\ MaxAttempts = 2
  /\ Q = "q0"
  /\ D = "d0"
===========================================================================
