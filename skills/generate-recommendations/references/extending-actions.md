# Extending recommendation actions

Read this file only when the existing action set cannot safely express the requested
change.

The documented actions are not a closed schema. If a task does not fit them cleanly, add
an action instead of forcing the change into a misleading existing action:

1. implement a parser, stale-state validator/planner, review output, and dry-run/apply
   executor in an appropriate backend module under `taxonomy/applicator/`;
2. register its action name in `scripts/apply_recommendations.py`;
3. reject duplicate or interacting mutations that cannot be applied safely;
4. add focused parsing, stale-state, idempotence, dry-run, and application tests; and
5. document the new action in the relevant model reference and link it from `SKILL.md`.

Keep specialized actions when they encode useful semantics or make a multi-record change
safer; generic actions are not a reason to discard those guardrails.
