# Per-Phase Implementation Workflow

A repeatable process for implementing, verifying, and committing each phase of the project. This document describes the **generic process** — phase-specific commands (how to run, what to verify) are already documented in each phase SPEC's "Operating commands" and "Acceptance criteria" sections; this file doesn't duplicate them.

---

## One-time setup (before Phase 1)

Done once, at the start of the project:

```bash
# 0. Prerequisite: PySpark/Spark Declarative Pipelines needs a JVM — not covered by pip/uv
java -version   # needs JDK 17 or newer (Spark 4.1+/SDP requirement — stricter than plain PySpark)
                # install one if this fails (e.g., via your OS package manager, or https://adoptium.net)

# 1. Virtual environment with uv
uv venv .venv --prompt cdcstream-wikipedia-project
source .venv/bin/activate   # or .venv\Scripts\activate on Windows

# 2. Initialize the uv project (if pyproject.toml doesn't exist yet)
uv init --no-readme --name cdcstream-wikipedia-project

# 3. Credentials: never hardcode — always .env
cp .env.example .env
# edit .env with real values (local MinIO, nothing sensitive in Phase 1)

# 4. Confirm .gitignore covers .env and .venv/
cat .gitignore   # should contain: .env, .venv/, __pycache__/, *.pyc
```

### Git identity (if this machine also has a work/enterprise GitHub account configured)

Claude Code's account (Anthropic authentication) and git/GitHub identity are two independent systems — Claude Code just runs `git`/`gh` commands through whatever the terminal already has configured. Scope this repo to a personal identity explicitly, rather than relying on whatever the machine's global git config points to:

```bash
# Repo-scoped identity (not global) — ensures commits attribute to the right account
git config user.name "Your Name"
git config user.email "your-personal-email@..."
```

If the machine's SSH/`gh` setup defaults to a work account, use a dedicated SSH host alias for this repo instead of fighting the global config:

```bash
ssh-keygen -t ed25519 -C "your-personal-email" -f ~/.ssh/id_ed25519_personal
# add the public key to github.com/settings/keys on your PERSONAL account
```

In `~/.ssh/config`:
```
Host github.com-personal
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ed25519_personal
```

```bash
git remote set-url origin git@github.com-personal:your-username/your-repo.git
gh auth login   # authenticate gh with the personal account
gh auth switch --hostname github.com --user your-personal-username
```

### `README.md`, `docs/CLAUDE.md`, and `docs/RUNBOOK.md` skeletons

All three start during setup, not at the end of the project — see Section 10.2 of `SPEC-agnostic-architecture.md` for why. Ask Claude Code to create all three in this same initial session, before Phase 1 begins:

```
Create a minimal README.md skeleton (project objective, tech stack
placeholder, architecture diagram placeholder, a phase-status checklist
for Phases 1-4), a baseline docs/CLAUDE.md (project one-liner, pointers
to docs/SPEC-agnostic-architecture.md and docs/ENGINEERING-PRINCIPLES.md,
and the non-negotiable conventions: Python + uv, .env for credentials,
English-only for everything in this repo), and a docs/RUNBOOK.md skeleton
with one section per phase (Phase 1 ... Phase 4), each with empty "Local
setup," "Local run," and "Cloud (optional)" subsections to be filled in
as each phase is implemented.
```

All three get updated as part of each phase's own PR going forward (step 11 below) — never as a separate end-of-project task, and never something you need to remember to ask for manually.

## Step by step, per phase


### 1. Create the phase branch from an up-to-date main

```bash
git checkout main
git pull
git checkout -b phase{N}-{short-name}
# e.g.: git checkout -b phase1-ingestion
```

`main` should always stay in a clean, complete state (a finished, acceptance-verified phase) — never with a half-implemented phase on it. This is also what makes the Phase 2.5 CI meaningful: it runs on every push/PR against `main`, giving an automated gate on top of the local checklist below.

### 2. Start the Claude Code session

Name the session after the branch (consistency, and easier to resume later — see Section 11 of `SPEC-agnostic-architecture.md`):

```bash
claude -n phase{N}-{short-name}
# e.g.: claude -n phase1-ingestion
```

### 3. Initial implementation prompt

Reference the documents by path, don't paste their content — Claude Code reads the files directly:

```
Implement Phase {N} as specified in docs/SPEC-phase{N}-{name}.md.

Required context before starting:
- docs/SPEC-agnostic-architecture.md (interface contracts, principles P0-P8, conventions)
- docs/ENGINEERING-PRINCIPLES.md (DRY/KISS/YAGNI/SOLID — how to apply with moderation)
- Specs from earlier phases already implemented, if this phase depends on them

Work incrementally: implement one component at a time, show the result, and
wait for my review before moving to the next. Also generate the tests
described in the SPEC's "Required tests" section.

Use uv for any new dependency (uv add <package>), never pip directly.
No hardcoded credentials — everything via .env + python-dotenv.

Before considering this phase done, fill in this phase's section of
docs/RUNBOOK.md: "Local setup" and "Local run" from the SPEC's "Operating
commands" section, worded as a first-time reader would need them (don't
assume context from this conversation). If this phase has an optional
cloud mode, also fill in "Cloud (optional)" with the cost warning and the
ready-to-paste cloud snippet per Section 10.1 of docs/SPEC-agnostic-architecture.md.
```

### 4. Incremental review

As each component is generated (e.g., a Handler, then the Ingestor, then the tests), review it before asking for the next one — don't let the session generate the entire phase at once with no intermediate checkpoint.

#### Pausing and resuming mid-phase

Closing the terminal, sleeping the machine, or stepping away doesn't lose anything — Claude Code saves the session transcript continuously to disk, per project directory. To pick back up exactly where you left off:

```bash
claude --resume phase{N}-{short-name}
```

This restores the full conversation history, including every tool call already made — there's no need to re-explain context or re-paste the initial prompt; just continue naturally (e.g., "continue where we left off," or ask directly for the next step). If you don't remember the exact session name, `claude --resume` with no argument opens an interactive picker.

If session history isn't available for some reason (expired, switched machines), `docs/PROGRESS.md` + `git log` are the durable fallback that doesn't depend on any conversation memory — see Section 11 of `SPEC-agnostic-architecture.md`.

### 5. Generate/update the dependency file

At the end of the phase's implementation, export dependencies to the committable format:

```bash
uv export --no-hashes --format requirements-txt -o requirements.txt
```

### 6. Local verification — checklist before any commit

This is the step you don't skip: **only commit after everything below passes.**

```bash
# 1. Environment active and dependencies installed
source .venv/bin/activate
uv sync   # or: uv pip install -r requirements.txt

# 2. Environment variables loaded (.env present and filled in)
cat .env   # check it isn't empty/outdated for this phase

# 3. Bring up the local stack needed for this phase (if applicable)
docker compose -f local-stack/docker-compose.yml up -d

# 4. Run this phase's test suite
pytest tests/ -v

# 5. Run the phase's operating command(s) — see the "Operating commands"
#    section of the corresponding SPEC-phase{N} (e.g., python -m src.producer.main)

# 6. Manually verify each item in the SPEC-phase{N} "Acceptance criteria"
#    section — not just "the tests passed," but each specific
#    Given/When/Then listed there

# 7. Tear down the local stack (no leftover process/cost)
docker compose -f local-stack/docker-compose.yml down -v
```

### 7. Commit — only after success is confirmed in step 6

```bash
git add .
git commit -m "feat(phase{N}): implement {phase name}"
```

Suggested granularity: one commit per reviewed component (step 4), not a single giant commit at the end — this preserves the commit history as an SDD-process narrative, which is part of this repository's portfolio value. This is also why the PR below is merged with a regular merge commit, not squashed.

### 8. Push the branch and open the PR

```bash
git push -u origin phase{N}-{short-name}
gh pr create --title "Phase {N}: {phase name}" --body "Implements docs/SPEC-phase{N}-{name}.md"
```

Wait for the Phase 2.5 CI `test` job to run green on the PR — that's the automated gate. Review the aggregated diff in the PR itself before merging; seeing the whole diff at once catches things that reviewing component-by-component during implementation doesn't.

### 9. Merge — regular merge commit, not squash

```bash
gh pr merge --merge
```

Squashing would collapse the per-component commit history from step 7 into one commit, destroying the granularity that's part of this repo's portfolio value.

### 10. Clean up the branch

```bash
git checkout main
git pull
git branch -d phase{N}-{short-name}
git push origin --delete phase{N}-{short-name}
```

Optional: `git tag phase{N}-complete` on `main` after merging — gives a navigable milestone for `docs/PROGRESS.md` to reference and for anyone browsing the repo on GitHub.

### 11. Update `docs/PROGRESS.md`, `README.md`, `docs/CLAUDE.md`, and `docs/RUNBOOK.md`

All four are part of the same PR, not an afterthought — the `docs/RUNBOOK.md` section for this phase should already exist from step 3, but double-check it here before opening the PR:

- **`docs/PROGRESS.md`**: three lines — current phase, what's already implemented, what's left — per Section 11 of `SPEC-agnostic-architecture.md`. This is what allows resuming from a brand-new session without depending on conversation memory.
- **`README.md`**: check off this phase in the status checklist; add any new tech-stack entry or architecture-diagram detail this phase introduced.
- **`docs/CLAUDE.md`**: append a short "what exists now" note (e.g., "Phase 1 complete: producer/consumer implemented, see `SPEC-phase1-ingestion.md`") — this is what a future Claude Code session reads automatically to orient itself, so keep it terse; it's not the place for the full story (that's `trade-offs.md`).
- **`docs/RUNBOOK.md`**: verify this phase's "Local setup"/"Local run"/"Cloud (optional)" subsections are actually complete enough for someone who wasn't in this conversation to follow — this is the check that catches the gap of having to ask for it manually after the fact.

### 12. Next phase

Repeat from step 1 for `Phase {N+1}`.

---

## Note on cloud execution (optional, per phase)

When a phase has an optional cloud mode (swapping `*_BACKEND` in `.env`), `docs/RUNBOOK.md` documents the specific steps and cost warnings — this generic workflow doesn't repeat them. The general rule: **always validate locally first** (step 6 above); only run the cloud mode after the local version has already been verified and committed.
