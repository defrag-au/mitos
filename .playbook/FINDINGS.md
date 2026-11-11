# Findings

Decisions on findings captured in this repository. Open observations live in
`.playbook/findings/`, which is not committed. The loop is documented in
`agent-playbook/docs/findings.md`.

When a finding is decided, append a section here and delete its file. Never rewrite an entry above.

```markdown
## 2026-11-05 · what the finding was
`fix-now` · rule `rules/org/defrag/<id>`
```

`fix-now` names the rule or tool change it produced, and the measure that would retire it.
`fix-later` names the condition that would trigger the work. `accept-as-is` and `reject` say why,
so the same observation is not raised again.
