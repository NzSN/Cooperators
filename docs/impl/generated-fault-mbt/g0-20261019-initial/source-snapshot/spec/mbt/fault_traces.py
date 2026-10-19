"""Strict conversion of this model's TLC counterexamples to projected ITF.

No eval, VIEW reduction, guessed effects, or parsing of arbitrary TLA+ programs.
Only finite values emitted by TLC for MBFaults are accepted.
"""
import json
import re


TOKEN = re.compile(r'\s*(<<|>>|\|->|:>|@@|[{}\[\](),]|-?\d+|"(?:[^"\\]|\\.)*"|[A-Za-z_][A-Za-z_0-9]*)')


class ValueParser:
    def __init__(self, source):
        self.tokens = []
        position = 0
        while source[position:].strip():
            match = TOKEN.match(source, position)
            if not match:
                raise ValueError(f"unsupported TLC value at {source[position:position+60]!r}")
            self.tokens.append(match[1])
            position = match.end()
        self.position = 0

    def take(self, expected=None):
        if self.position == len(self.tokens):
            raise ValueError("incomplete TLC value")
        value = self.tokens[self.position]
        self.position += 1
        if expected is not None and value != expected:
            raise ValueError(f"expected {expected}, found {value}")
        return value

    def peek(self):
        return self.tokens[self.position] if self.position < len(self.tokens) else None

    def collection(self, end):
        values = []
        if self.peek() != end:
            values.append(self.value())
            while self.peek() == ',':
                self.take(',')
                values.append(self.value())
        self.take(end)
        return values

    def value(self):
        token = self.take()
        if token.startswith('"'):
            return json.loads(token)
        if re.fullmatch(r'-?\d+', token):
            return int(token)
        if token in ('TRUE', 'FALSE'):
            return token == 'TRUE'
        if token == '<<':
            return self.collection('>>')
        if token == '{':
            values = self.collection('}')
            return {'#set': sorted(values, key=lambda item: json.dumps(item, sort_keys=True))}
        if token == '[':
            record = {}
            while self.peek() != ']':
                name = self.take()
                name = json.loads(name) if name.startswith('"') else name
                if name in record:
                    raise ValueError(f"duplicate record field {name}")
                self.take('|->')
                record[name] = self.value()
                if self.peek() != ',':
                    break
                self.take(',')
            self.take(']')
            return record
        if token == '(':
            entries = []
            while True:
                key = self.value()
                self.take(':>')
                entries.append([key, self.value()])
                if self.peek() != '@@':
                    break
                self.take('@@')
            self.take(')')
            keys = [json.dumps(key, sort_keys=True) for key, _ in entries]
            if len(keys) != len(set(keys)):
                raise ValueError("duplicate function key")
            return {'#map': entries}
        raise ValueError(f"unsupported TLC token {token}")

    def parse(self):
        value = self.value()
        if self.position != len(self.tokens):
            raise ValueError(f"trailing TLC tokens: {self.tokens[self.position:]}")
        return value


def to_itf(value):
    if type(value) is int:
        return {'#bigint': str(value)}
    if isinstance(value, (list, tuple)):
        return [to_itf(item) for item in value]
    if isinstance(value, dict):
        return {key: to_itf(item) for key, item in value.items()}
    return value


def project_trace(text, scenario, goal, bound):
    if f'Invariant {goal} is violated.' not in text:
        raise ValueError(f"missing expected coverage counterexample: {goal}")
    states = []
    blocks = re.split(r'(?m)^State \d+:.*\n', text)[1:]
    for index, block in enumerate(blocks):
        block = re.split(r'(?m)^(?:[\d,]+ states generated|Finished in |Error:|Progress:)', block)[0]
        matches = list(re.finditer(r'(?m)^/\\ (\w+) = ', block))
        fields = {}
        for number, match in enumerate(matches):
            name = match[1]
            if name not in {'action_taken', 'parameters', 'result', 'observation', 'steps'}:
                continue
            end = matches[number + 1].start() if number + 1 < len(matches) else len(block)
            if name in fields:
                raise ValueError(f"duplicate state variable {name}")
            fields[name] = ValueParser(block[match.end():end]).parse()
        if set(fields) != {'action_taken', 'parameters', 'result', 'observation', 'steps'}:
            raise ValueError(f"incomplete trace state {index}: {fields.keys()}")
        if fields.pop('steps') != index or fields['parameters']['scenario'] != scenario:
            raise ValueError("noncontiguous or cross-profile counterexample")
        if scenario == 'identity':
            for name in ('ids', 'replies', 'currentTokens', 'kinds', 'labels'):
                value = fields['observation'][name]
                if '#map' not in value:
                    fields['observation'][name] = {'#map': sorted(value.items())}
        states.append({'#meta': {'index': index}, **to_itf(fields)})
    if len(states) < 2 or len(states) - 1 > bound:
        raise ValueError(f"invalid witness length: {len(states)} states, bound {bound}")
    if states[0]['action_taken'] != 'init' or states[0]['result'] != 'none':
        raise ValueError("counterexample does not start at initialization")
    return {'#meta': {'format': 'ITF', 'source': 'TLC MBFaults coverage counterexample',
                     'scenario': scenario, 'goal': goal, 'transitionBound': bound,
                     'projection': 'Observation only; internal state and ghost receipts omitted'},
            'params': [], 'vars': ['action_taken', 'parameters', 'result', 'observation'],
            'states': states}
