# Commit Style — Conventional Commits (MANDATORY)

Every commit message MUST follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description> (#N)

<optional body>

Closes #N
```

- **Allowed types**: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `ci`, `perf`, `style`, `build`, `revert`
- **Scope** matches a crate or area: `feat(fuse):`, `fix(meta):`, `refactor(storage):`, `docs:`
  (crates: `fuse` · `vfs` · `meta` · `storage` · `types` · `common` · `utils` · `binary`)
- **Breaking changes** use `!`: `feat(meta)!: change on-disk key layout`
- Include the `(#N)` issue reference in the commit subject.
- Include `Closes #N` in the commit body so the issue auto-closes on merge.
- A local `commit-msg` hook (`scripts/check-conventional-commit.sh`, wired via
  `.pre-commit-config.yaml`) enforces this — do NOT bypass it with `--no-verify`.
- Do NOT use free-form messages like `"update code"` or `"fix stuff"` — they are rejected.

## Setup

```bash
prek install                       # pre-commit hooks (the Rust quality gate)
prek install --hook-type commit-msg  # the Conventional Commits check
```
