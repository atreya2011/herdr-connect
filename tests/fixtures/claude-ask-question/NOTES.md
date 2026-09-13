# Live multiSelect probe (review item 1)

`encode_claude_question_decision` used to put a `QuestionAnswer::Multiple` answer on the wire as a
JSON array (`"answers":{"Which toppings?":["Cheese","Mushrooms"]}`), but Claude Code's real
`AskUserQuestion` `answers` schema is `{additionalProperties: {type: "string"}}`, and Claude's own
dialog records a multiSelect answer as one comma-joined string. The fix (`format_question_answer`
in `src/question.rs`) formats every answer as a string before it goes on the wire. This was proven
live against a real Claude Code process before shipping, per the review's requirement, since the
runtime effect of the old, schema-invalid shape was otherwise unverified (accepted? coerced?
rejected?).

## Method

Real `claude` CLI (`2.1.270`, `--model haiku`), run interactively in a real Herdr pane (headless
`--print` mode was tried first and does not expose the `AskUserQuestion` tool at all, independent
of hooks, so an interactive pane -- the same mechanism the existing real-row tests in `src/main.rs`
already use -- is what "run directly" resolves to here; no Discord, no broker, no
`herdr-connect-rs` binary involved). cwd was the fixed, pre-trusted `claude_testrun_dir`
(`~/.cache/herdr-connect-testrun/claude`), never this repository, per the owner's laws.

The hook itself was a small standalone Python script (not this crate's own `hook` subcommand --
the point is to observe Claude's own acceptance of the wire shape, isolated from this crate's other
code) registered as the `PreToolUse` `AskUserQuestion` matcher via `--settings`:

```python
#!/usr/bin/env python3
import json, sys

payload = json.loads(sys.stdin.read())
if payload.get("tool_name") != "AskUserQuestion":
    sys.exit(0)

tool_input = payload.get("tool_input", {})
questions = tool_input.get("questions", [])
answers = {}
for question in questions:
    options = question.get("options", [])
    if question.get("multiSelect") and len(options) >= 3:
        chosen = [options[0]["label"], options[2]["label"]]
        answers[question["question"]] = ", ".join(chosen)  # the fix: a joined STRING
    elif options:
        answers[question["question"]] = options[0]["label"]

updated_input = dict(tool_input)
updated_input["answers"] = answers
print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": "allow",
        "updatedInput": updated_input,
    }
}))
```

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "AskUserQuestion",
        "hooks": [{"type": "command", "command": "python3 <path to the script above>", "timeout": 30}]
      }
    ]
  }
}
```

Steps: `herdr tab create` a fresh pane at `claude_testrun_dir`, `herdr agent start ... --kind
claude -- --model haiku --settings <settings.json>`, then `herdr agent prompt` forced Claude to ask
one multiSelect question ("Which toppings?", options Cheese/Olives/Mushrooms, `multiSelect: true`)
and to do nothing else.

## Observed

The pane settled at `done` -- it never blocked on Claude's own dialog. The pane's own rendered
output:

```
● User answered Claude's questions:
  ⎿  · Which toppings? → Cheese, Mushrooms

● You selected Cheese and Mushrooms as your toppings.
```

The tool result carried no error (`is_error` absent/`None`). The real session transcript
(`~/.claude-one/projects/-home-user--cache-herdr-connect-testrun-claude/<session>.jsonl`)
recorded:

```json
{
  "toolUseResult": {
    "questions": [
      {
        "question": "Which toppings?",
        "header": "Toppings",
        "options": [
          {"label": "Cheese", "description": "Add cheese"},
          {"label": "Olives", "description": "Add olives"},
          {"label": "Mushrooms", "description": "Add mushrooms"}
        ],
        "multiSelect": true
      }
    ],
    "answers": {"Which toppings?": "Cheese, Mushrooms"}
  }
}
```

`answers["Which toppings?"]` is the plain string `"Cheese, Mushrooms"` -- exactly the shape
`format_question_answer` now produces, exactly what Claude's own dialog would have written, and
exactly what Claude accepted with no input-validation failure. The pane, its Herdr tab, and the
transcript file were cleaned up after the probe; nothing from it is left behind.
