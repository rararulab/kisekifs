## What & why

<!-- Explain your change as a story a reviewer can follow WITHOUT reading the diff first:
  - What problem or goal prompted this? (the issue's Intent — don't just restate the title)
  - What approach did you take, and *why this one*? Name the key design decision.
  - What did you consider and reject, or what non-obvious constraint/tradeoff shaped it?
  The diff shows WHAT changed. This section must explain WHY it looks like this. -->

## Type of change

<!-- Add the matching type label to this PR -->

| Type | Label |
|------|-------|
| Bug fix | `bug` |
| New feature | `enhancement` |
| Refactor | `refactor` |
| CI / Infrastructure | `ci` |
| Maintenance | `chore` |
| Documentation | `documentation` |

## Component

<!-- Which crate(s) does this touch? -->
<!-- `fuse` · `vfs` · `meta` · `storage` · `types` · `common` · `utils` · `binary` -->

## Closes

<!-- REQUIRED: link the issue this PR resolves; merge auto-closes it. -->
<!-- If no issue exists, create one first: gh issue create -->

Closes #

## How to verify

<!-- Commands you ran or steps a reviewer can repeat — evidence, not a command dump. -->
- [ ] `just check` / `just lint` pass
- [ ] `cargo test -p <crate>` (or `just test`) as relevant
- [ ] Mounted acceptance (`just test-mounted`) when the change touches the mounted path
- [ ] Tested locally (say how)
