# Derived-template plan review record

Date: 2026-09-06.
Plan: [Composable cached database scenarios](derived-templates.md).
Source baseline: `8a460ca`.

This record distinguishes planning checks from implementation validation.
No feature code or runtime tests were implemented or executed during planning.
Reviews use the available inherited reasoning agents in this session.
No GPT Pro web session or external multi-model review is claimed.

## Grounding completed before review

- Read the public API, fingerprint format, cache/coordination paths, metadata,
  admission, source ownership, tests, contributor guidance, and CI commands.
- A separate source investigation confirmed detached blocking-worker lifetime,
  current abandoned-initializer recovery, and strict metadata compatibility.
- Verified PostgreSQL 18 cloning constraints against primary documentation.
- Verified blocking-task cancellation against the Tokio 1.53.1 documentation,
  matching the repository lockfile.
- `bv --robot-triage` and `br list --status=open --json` showed no open delivery
  work at the start of planning.
- The initial git worktree was clean.

## Independent identity calculation

A one-off Python standard-library calculation used `hashlib.sha256` and
`struct.pack('>Q', len(frame))` to check the proposed recipe.
No production Rust function exists yet or participated in this calculation.

| Field | Value |
| --- | --- |
| Domain | `postgres-test-harness-derived-template-v1` |
| Parent | `000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f` |
| Step | `202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f` |
| Frame lengths | 41, 32, 32 bytes |
| Total framed input | 129 bytes |
| Digest | `678f8b338e81a48db8625fd8ab6c949a2852e6aec6261d710976de68381b7cd5` |

## Review rounds

Round findings and resolutions are recorded here as each review completes.
Intermediate before/after snapshots are local scratch artifacts under
`target/planning-review/`; they are not additional delivery requirements.

### Round 1 — Architecture, lifecycle, and test oracles

Reviewer: `plan_review_1`, fresh context, complete 1,428-line draft and relevant
repository paths read before its final response.

Accepted revisions:

- Require a live-server terminal-admission regression. A removed owned server
  would fail even if the admission check had been accidentally bypassed.
- Require a Pending-poll registration signal for local waiters before
  interrupting their flight owner; starting a task alone proves no wakeup.
- Make DT3 self-contained with the full callback signature, named test helpers,
  advisory-key domains, and a concrete finalization gate sequence.
- Clarify that a ready hit avoids database CREATE, while cloning an Arc source
  handle before cache lookup is allowed.
- Integrate independent root findings: fixed fingerprint vector, zero-age stale
  sweep instead of metadata rewrites, and exact benchmark template inventory
  to replace the old fixture-count cleanup expectation.
- Specify benchmark strategy costs, counts, timing exclusions, and JSON fields.

Validation after integration:

- Self-containment: isolated DT3 initially failed; expanded the task body with
  the missing signature and synchronization mechanisms for fresh recheck.
- Graph: reviewer checked `DT1 → DT2 → {DT3, DT4, DT5, DT6}` as acyclic; each
  terminal task contributes to the epic's explicit completion contract.
- Justification: source ownership, shared acquisition, unchanged metadata,
  callback bounds, and initializer permit policy all had adequate rationale.
- Steady state: substantive test/report specifications changed; another round
  was required. Saved the actual diff as local review scratch.

### Round 2 — Synchronization and measurement contracts

Reviewer: `plan_review_2`, fresh context, complete 1,593-line revised plan and
relevant source inspected.

Accepted revisions:

- Finalization must be observed as the exact child's Ready comment on an
  active backend blocked by the controlled catalog lock. The existing helper's
  last-query text can otherwise match an earlier Initializing comment.
- Await a cancelled task's join result while the worker remains gated before
  attributing source protection to detached worker ownership.
- Embed assigned V-case recipes in delivery descriptions and section 11B in
  DT6, keeping synchronization and measurement detail inside the beads.

Validation after integration:

- Self-containment: isolated DT3 passed with source inspection allowed;
  the extracted DT6 task includes the full report/timing appendix.
- Graph: the same acyclic six-task graph remained valid; shared CI file edits
  in DT4/DT5 do not imply a runtime prerequisite.
- Justification: fingerprint domain, shared acquisition, worker ownership,
  metadata compatibility, and benchmark inventory all passed rationale checks.
- Steady state: marginal synchronization changes only; API, architecture,
  scope, and dependency edges remained stable.

### Round 3 — Prewarm isolation and ownership oracles

Reviewer: `plan_review_3`, fresh context, isolated extracted DT4 followed by
the complete 1,611-line plan and focused source inspection.

Accepted revisions:

- Use prewarm capacity one for the dirty-return test. At larger capacity,
  a pristine second initial slot could hide a refill from the wrong source.
- Gate the first/only initial-fill slot, with no other pool retaining its
  child. Cleanup of an earlier ready slot could otherwise retain the source
  independently and mask a missing owner in the detached creation worker.
- Correct the private fingerprint prototype to spell `pub(crate) fn` for its
  use from the harness module.

Validation after integration:

- Self-containment: extracted DT4 passed independently; its updated recipes
  now constrain both alternate sources of false-positive evidence.
- Graph: no cycles, hidden runtime dependencies, or ownerless V cases found.
- Justification: separate fingerprint framing, shared acquisition, worker
  ownership, live-server admission, and benchmark accounting remained sound.
- Steady state: two narrow test-input constraints and one visibility spelling;
  no architectural or task-graph changes.

### Round 4 — Final execution-readiness check

Reviewer: `plan_review_4`, fresh context, isolated extracted DT6 followed by
the complete revised plan and targeted source inspection.

No correctness, contract, dependency, or validation blocker was found.
No further revision was requested.

Validation:

- Self-containment: isolated DT6 passed with its workload, timing, counts,
  inventory, schema, cleanup, and validation contracts carried in the bead.
- Graph: acyclic, no orphan outcomes or hidden prerequisite edges.
- Justification: identity compatibility, callback flexibility, worker-owned
  source lifetime, prewarm oracles, and performance accounting all passed.
- Steady state: reached. This final complete review requested no changes;
  the preceding two rounds made only narrow validation refinements.

All four planned rounds are complete. Subsequent changes only materialize the
reviewed task graph and record IDs/status; they do not change feature design.

## Delivery graph validation

Created epic `postgres-test-harness-j82` and children `.1` through `.6`.
Performed six focused delivery-graph checks after conversion:

1. Outcome scope: one epic and six concrete implementation, regression,
   documentation, and measurement outcomes; no planning or review beads.
2. Dependency fidelity: `br dep list` readback matches five blocking edges and
   six parent-child edges exactly, with no reverse epic prerequisites.
3. Cycles and order: `br dep cycles` and `bv --robot-insights` report no cycles;
   the dependency order is identity, shared implementation, then four outcomes.
4. Readiness: `br ready --epic postgres-test-harness-j82` returns only `.1`;
   `bv --robot-triage` also names `.1` as the top pick.
5. Task contracts: every saved description and acceptance field matches the
   reviewed file-backed payload byte-for-byte; priorities, types, labels, and
   open status are correct. Assigned V cases and DT6's appendix survived.
6. Export and scope: `br sync --flush-only` completed; `br sync --status`
   reports zero dirty records, 41 exportable issues, and no coverage drift.
   Git shows seven issue lines added and no old issue lines removed.

`bv`'s dependency-only insight calls the epic and initial ready task “Orphans”
because they have no blocking prerequisites. They are intentional graph roots:
DT1 has DT2 as a consumer, and all six tasks have explicit epic membership.
No artificial blocking edges were added to alter that metric.
The graph export contains seven nodes and eleven typed relationship edges.

The canonical export path was obtained with `br info --json` and supplied to
`bv --db` for the explicit graph checks. Ordinary `bv --robot-triage` was also
verified to see the same seven open issues after sync.
No internal issue JSONL records were parsed to infer dependencies.

All delivery tasks remain open and unclaimed; no implementation status was
changed to imply work had begun.
The new local Beads database migration-state sidecar is ignored alongside
existing database sidecars in `.beads/.gitignore`.

Final artifact checks: balanced Markdown fences, six task sections, 25 assigned
regression recipes, existing source anchors present, and `git diff --check`
clean. No Rust tests were run because this session changed planning/tracking
artifacts only; the plan states the future implementation validation commands.
