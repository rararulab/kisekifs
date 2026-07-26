#!/bin/sh
# Guard: prevent git checkout/switch on the main worktree.
# Agents must use git worktree for branch work, never switch main.

# Read the tool input from stdin
INPUT=$(cat)

# Extract the command from the JSON payload
COMMAND=$(echo "$INPUT" | grep -o '"command":"[^"]*"' | head -1 | sed 's/"command":"//;s/"//')

# Check if we're on the main worktree (not inside .worktrees/ or .claude/worktrees/)
CURRENT_DIR=$(pwd)
case "$CURRENT_DIR" in
  */.worktrees/*|*/.claude/worktrees/*) exit 0 ;;  # Inside a worktree — allow
esac

# Check if the command is git checkout or git switch to a different branch
case "$COMMAND" in
  *"git checkout -b"*|*"git switch -c"*|*"git checkout -B"*)
    echo "BLOCKED: Do not create branches on the main worktree. Use 'git worktree add' instead."
    echo "Example: git worktree add .worktrees/issue-N-name -b issue-N-name"
    exit 2
    ;;
  *"git checkout "*|*"git switch "*)
    # Allow 'git checkout main', 'git checkout -- file', 'git checkout --ours/--theirs'
    case "$COMMAND" in
      *"git checkout main"*|*"git checkout origin/"*) exit 0 ;;
      *"git checkout -- "*|*"git checkout --"*) exit 0 ;;
      *"git switch main"*|*"git switch -"*) exit 0 ;;
    esac
    echo "BLOCKED: Do not switch branches on the main worktree. Use 'git worktree add' for branch work."
    echo "Example: git worktree add .worktrees/issue-N-name -b issue-N-name"
    exit 2
    ;;
esac

exit 0
