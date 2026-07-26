# Development Workflow — Issue → Worktree → Local Commit → Verify → Review → Push → PR → Merge

**Every code change — no matter how small — MUST follow this workflow.**
Single-line fixes, typo corrections, config tweaks, doc updates, and refactors
all go through the flow below. Agents must NEVER directly edit source files on
the `main` branch, and never commit to `main`. A `guard-main-branch` hook
(`.claude/hooks/guard-main-branch.sh`) blocks branch work on the main checkout,
and `main` is a protected branch on GitHub (PR + green CI required to merge).

This workflow is adopted from the reference repo
[`rararulab/rara`](https://github.com/rararulab/rara) and adapted to KisekiFS. It
is built for **parallel multi-agent development**: many agents each own an
isolated worktree + branch + PR, verified/reviewed/merged independently.

```
0. ISSUE          →  an issue exists first (gh issue create), labelled by type
                     + component. It carries Intent, prior art, decisions,
                     boundaries, and a `Verify:` recipe.
1. WORKTREE       →  git worktree add .worktrees/issue-N-<slug> -b issue-N-<slug>
2. IMPLEMENT      →  read the code; make the smallest change; run the quality
                     gate; commit LOCALLY (Conventional Commits; do not push)
3. VERIFY         →  fresh-context check from clean state: re-run the gate,
                     and for mounted-path changes drive the real mount
                     (`just test-mounted`); record evidence
4. REVIEW         →  review the worktree diff against the issue (loop to APPROVE)
5. PUSH + PR      →  push the branch; gh pr create; gh pr checks --watch
6. MERGE          →  gh pr merge --squash --delete-branch (green CI + APPROVE)
7. CLEANUP        →  git worktree remove + git branch -D
```

> **Multi-agent roles.** The subagent contracts that run these stages —
> `spec-author`, `implementer` / `implementer-backend`, `verifier`, `reviewer`,
> `debugger` — live in `.claude/agents/*` and `harness/roles/*`, with the stage
> protocol in `docs/guides/pipeline.md`. Those land in the follow-up
> "agent roles" PR; this document is the human/git-level contract they follow.

## Step 0: Issue first

No worktree without an issue. `gh issue create` with a type label (`bug`,
`enhancement`, `refactor`, `chore`, `documentation`) and a component hint
(`fuse` · `vfs` · `meta` · `storage` · `types` · `common` · `utils` · `binary`).
The issue body states the Intent (not just a title restatement), any prior art
(`gh issue list`, `git log --grep`, `rg`), the key decisions, the boundaries
(which paths may change), and a `Verify:` recipe a reviewer can repeat.

## Step 1: Worktree

```bash
git worktree add .worktrees/issue-{N}-{slug} -b issue-{N}-{slug}
```

Every edit happens inside the worktree. The main checkout is never edited
in-place and never switched to a feature branch (the guard hook enforces this).

## Step 2: Implement

1. Read the actual code the change touches (and the issue).
2. Make the smallest change that satisfies the issue.
3. Run the **quality gate** (see below).
4. Commit locally with a Conventional Commits subject + `Closes #N` in the body
   (see [commit-style.md](commit-style.md)). Do NOT push yet.

If the change touches the mounted data path (`components/fuse`,
`components/vfs`, `components/meta`, `components/storage`), add or extend a
mounted acceptance case (see `docs/src/posix-support.md` and the
`mounted::*` tests) in the same change.

### Quality gate

```bash
just check      # cargo check --all --all-features
just lint       # clippy -D warnings + cargo doc
just fmt        # cargo +nightly fmt (+ taplo + hawkeye)
cargo test -p <crate>          # targeted tests for what you changed
just test-mounted              # mounted acceptance, when the mounted path changed
```

Or run the pre-commit hooks directly: `prek run --all-files`. The **final**
commit must pass all hooks; intermediate commits during development need not.
Do NOT use `--no-verify`.

## Step 3: Verify (independent, from clean state)

Re-run the gate from a clean worktree state, and for any change to the mounted
path, cold-drive the real filesystem — mount it and exercise the changed
behavior end-to-end (both sides of any write→read wiring), not just unit tests.
The `Test / Mounted Linux Acceptance` gate (`just test-mounted`) refuses to skip
when `/dev/fuse` or `fusermount3` is missing. Record the evidence (commands +
outputs) for the PR body. Verify and review catch disjoint failure classes —
verify runs the artifact, review reads the diff — so both exist and verify runs
first.

## Step 4: Review (BEFORE push)

Review the worktree diff (`git -C <worktree> diff origin/main..HEAD`) against the
issue: correctness, scope creep, and a cross-file regression-decision check
(`git log --since=30.days` on touched files — did a recent commit deliberately
remove/restructure what this re-introduces?). Loop until APPROVE; fixes are new
commits in the worktree (no amend/force-push after review).

## Step 5: Push + Open PR + Watch CI

Only after review APPROVE:

```bash
git -C <worktree> push -u origin issue-{N}-{slug}
gh pr create --base main \
  --title "<type>(<scope>): <description> (#N)" \
  --body "..." --label "<type>"
gh pr checks {PR} --watch
```

The PR body uses `.github/PULL_REQUEST_TEMPLATE.md` and must include the verify
evidence and `Closes #N`. The required merge-gate checks on `main` are the
`ci.yml`-routed aggregates **`Lint / Lint Success`**, **`Rust / Rust Success`**,
**`Test / Rust Test (Ubuntu)`**, and **`Test / Mounted Linux Acceptance`**.
Path filtering means docs-only PRs satisfy the code checks as `skipped`.

If a check fails: read the log, fix in the worktree, push again. Do not
`#[ignore]` tests to make CI green. For a genuine flake (same test failed
recently on `main`): `gh run rerun <id> --failed`, capped at 1.

## Step 6: Merge

Green CI + APPROVE'd review = merge.

```bash
gh pr merge {N} --squash --delete-branch
```

`--squash` makes the `main` commit match the Conventional Commit subject.

## Step 7: Cleanup

```bash
git worktree remove .worktrees/issue-{N}-{slug}
git branch -D issue-{N}-{slug}
```

## Confirmation gates

The parent agent chains through the steps without re-asking, EXCEPT:

- **(a) Merging to `main`.** Always ask before the final merge, even with green
  CI and an APPROVE.
- **(b) Destructive git operations.** `git reset --hard`, force-push,
  `branch -D` on a shared branch — anything that rewrites/discards history.

Everything else (status queries, step transitions inside an approved plan,
routine `git add`/`commit`/`gh pr create`, label tweaks) runs without a
confirmation round-trip.

## Parallel execution

For independent changes, split into separate issues at step 0 and run
implementer subagents in parallel — each with its own worktree, branch, and PR,
verified/reviewed/merged independently. Isolation for concurrent mounted tests:
per-run temp data dirs and unique mount points; never share a `/dev/fuse` mount
across parallel runs.

## Branch protection & CI-outage override

`main` protection (require PR, require the four status checks, no direct/force
push) is configured out-of-band by a repo admin. During a genuine CI outage,
an admin may temporarily relax the required checks to merge a fully
locally-verified PR, then restore them immediately — this is an emergency
override, not part of the normal flow.
