# github-core-clone-01 v3c — EVIDENCE

**Verdict: HOLD — do not submit. The last slot stays with v2.**

- Candidate: `~/workspace/makefun/github-core-clone-01-v3c/`, local HEAD `7a4197d`
  ("v3c: snapshot forge — write-time precomputed responses, sync dispatch,
  depth-fetch fix"), base `657faa2` (v3b) / `ac34932` (v2 @ ab548da).
  `EVIDENCE.md` itself is untracked (not part of the graded code commit).
- No GitHub repo created, no bundle, no submission — parent handles those.
- Rule applied (task): SHIP only if local mean CLEARLY beats v2's ~983 AND clone p50
  is not worse than v2's. The score bar fails under every reading (see grade-local).

## What changed (a different architecture, not another tweak of the clone pipeline)

**"Snapshot forge": reads are precomputed at write time and served as stored buffers.**

- **Snapshots (Repo):** `repoJsonBuf`, `branchesBuf`, `refsBuf` — the serialized REST
  forms of a repo, its branch list and its ref list, built once and invalidated on
  mutation (`touchGit` clears branches/refs; `touchRepoJson` clears the repo JSON
  from repo PATCH, issue create, issue PATCH state change, GraphQL closeIssue).
  Static one-time buffers for the fixed endpoints (`HEALTHZ_BUF`, `ROOT_BUF`,
  `META_BUF`, `USER_BUF`). Issue create/PATCH answers come from the memoized
  `issueJsonBuf` instead of a fresh stringify.
- **Synchronous dispatcher:** `handleForgeRequest` is now a plain sync function.
  Bodyless reads (healthz, git advertisements, bodyless REST GET/HEAD/DELETE — the
  timed read workload is 2,000 of those in a row) run straight through with no async
  frame and no microtask hops. Paths that need a body delegate to small async helpers
  (`handleGitUploadPack`, `handleGitReceivePack`, `handleMcpRequest`,
  `handleGraphqlRequest`, `handleRestWithBody`) that keep the exact same check order
  (404/auth before the body read).
- **Hot-path allocation cuts:** query strings parsed lazily (`LazyQuery`, only
  `.get()` is ever called — the read path carries no query string at all);
  precomputed `AUTH_HEADER` instead of a per-request template string; `readBodyFast`
  returns the transport's complete body buffer directly instead of a `Buffer.concat`
  copy on every POST; `sendJson`/`sendJsonBuf` skip the spread when there are no
  extra headers.
- **Transport:** `RawRes.end` sends large bodies (>= 64 KiB, e.g. full-clone packs)
  as two writes instead of copying megabytes to join head + body; small bodies keep
  the single-write path.
- **fetchClosure depth fix:** a depth-limited fetch no longer walks the whole object
  graph with `reachable()` to build a closure the depth branch throws away — on a
  depth-1 clone of the 200-commit repo that was a 600-object walk per cache miss.
  The full walk now runs only when `depth` is undefined, where its result is used.
- No handler, status code, header, or response byte changed from v2/v3b (verified:
  replay state.json byte-identical, see below).

## Live state re-checked 16 Nov ~05:35 WIB (the brief's numbers are stale)

- Leaderboard: 3,947 entries. Top score is **953** (perf 153, p50 387,494 µs).
  **Nobody scores >= 983.** The ~983 figure is a 14 Nov quiet-VM *local* grade,
  never a live score.
- Our live v2 ("Deft Nova v2 — memoized clone", submission 01a1d9ad-ee44…):
  **rank 727, score 930, perf 130, p50 540,139 µs** — not rank 577/2772.
- Prize zone is top 20 ($100/$70/$50/$20 x7/$14 x10); rank 10 sits at ~945.
- Live v2 (930) matches local-clean v2 (930.7) almost exactly, so local-clean
  numbers transfer to live unusually well for this competition.

## Smoke tests (28 checks, all pass)

`smoke.mjs`: healthz (+ request-id), REST 401/404/malformed-JSON, user/root/meta,
repo create/get/PATCH (snapshot + invalidation), issue create/get/PATCH
(open_issues_count 1 -> 0), branches, HEAD, git refs, GraphQL viewer +
createIssue, MCP tools/list (8) + create_issue, git info/refs v2 + receive-pack,
chunked POST (raw socket). Same script passes identically against v2 (the one
HEAD expectation — 404 with empty body — is v2's own behavior, not a v3c change).

## A/B — kit bench driver, interleaved v2 <-> v3c (3 paired rounds)

Run under load average 8.44 (a sibling agent's `house.mjs` at 40% CPU + the
markdown recorder): per-round perf points swung 39–112 and the wins swapped by
round. Means: v2 69.0 pts / p50 1,336,262 µs; v3c 67.3 / 1,316,832 — a wash
(-1.7 pts, -1.5% p50). The driver measures VM contention at that load, not code.
The grade-local runs below are the verdict metric.

## Grade-local — the verdict metric (FRESH kit, `--driver process --worktree --instances 3`)

Fresh kit: `~/workspace/makefun/kits-fresh/github-core-clone-01-kit-fresh/`
(bundle `e20a304d…`, harness `91e7d38c`, scorer `e07d784d`). Six runs, 18 trials,
interleaved v3c/v2. Feature 600/600, agent 200/200, state.json `a0e98d49…`
byte-identical to v1/v2/v3b, L1 regrade reproduced in all six runs.

| run | trials (score) | mean | perf | mean p50 µs | push | clone | read | write |
|---|---|---|---|---|---|---|---|---|
| v3c (run 1, LOAD-SPIKED) | 866 / 855 / 846 | **855.7** | 55.7 | 1,502,006 | 2227 | 566 | 5191 | 838 |
| v2 (run 1) | 931 / 931 / 932 | **931.3** | 131.3 | 519,606 | 881 | 193 | 1735 | 269 |
| v3c-2 (clean) | 938 / 949 / 945 | **944.0** | 144.0 | 437,482 | 560 | 199 | 1480 | 229 |
| v2-2 (clean) | 940 / 925 / 932 | **932.3** | 132.3 | 516,750 | 781 | 200 | 1591 | 310 |
| v3c-3 (clean) | 944 / 943 / 952 | **946.3** | 146.3 | 430,087 | 639 | 153 | 1630 | 233 |
| v2-3 (clean) | 933 / 923 / 931 | **929.0** | 129.0 | 540,404 | 773 | 266 | 1801 | 251 |

**Clean 6-trial totals (runs 2+3, the protocol's >= 6 trials without the spike):**
v3c mean **945.2**, p50 **433,784 µs** (push 600 / clone 176 / read 1555 / write 231);
v2 mean **930.7**, p50 **528,577 µs** (push 777 / clone 233 / read 1696 / write 281).
v3c beats v2 on all four workloads, wins 5 of 6 head-to-head trials, and the two
independent clean runs agree (944.0 and 946.3 vs 932.3 and 929.0).

**All 9 trials per candidate (including the spike):** v3c 915.3 / p50 789,858;
v2 930.9 / p50 525,587.

### The load spike in v3c run 1 (why it is set aside, not hidden)

Run 1's trials took 41.6 / 42.9 / 54.9 s vs 14–18 s for every other run: a
sibling agent's `python3 selfplay.py` (tiny-chess) started 22:45 at ~52% CPU
exactly as run 1 began, load average 7.25, and the run's own p50s trend up
across trials (1.28M -> 1.51M -> 1.71M) as the hog bit harder. The v2 run
minutes later (load fallen to 1.58) took 16–18 s. The spike is an external
artifact, not a code regression: v3c run 2, minutes later under clean load,
is the fastest run of the session. Both the spiked and the clean totals are
reported above; the verdict does not depend on which is used (see below).

## Verdict: HOLD

- **Score bar fails:** v3c's local mean is 945.2 clean (915.3 over all 9 trials) —
  clearly better than same-session v2 (930.7 clean), but NOT clearly above **983**.
  The 983 reference does not reproduce: same-session v2 grades 930.7 clean, and no
  live entry scores >= 983 (top is 953). No server-side change can clear a bar the
  reference code itself cannot reproduce on this VM.
- **Clone-p50 bar passes on clean runs** (176 µs vs v2's 233) but fails on the
  spiked 9-trial total (306 vs 220) — either way the score bar is the binding one.
- v3c is the strongest local entry measured in this session, and by the
  live-transfer seen for v2 (local-clean 930.7 ~= live 930) a live v3c at ~945
  would sit around rank 10, at the edge of the prize zone — but that is a
  judgement for the coordinator, not this build's bar. **Do not burn the last
  slot on it under the stated rule.**

## Where the evidence lives

- Grade-local runs: `~/workspace/makefun/gcc-v3c-evidence/grade-local-v3c{,-2,-3}/`
  (v3c) and `~/workspace/makefun/gcc-v3c-evidence/grade-local-v2{,-2,-3}/` (v2),
  with `.txt` transcripts alongside.
- Profiling harness (not part of the repo): `/tmp/v3cprof/smoke.mjs`,
  `/tmp/v3cprof/runner.mjs`, `/tmp/v3cprof/drive.mjs`, `/tmp/v3cprof/grade*.sh`.
- Fresh kit: `~/workspace/makefun/kits-fresh/github-core-clone-01-kit-fresh/`.
- This file: `~/workspace/makefun/github-core-clone-01-v3c/EVIDENCE.md` (untracked).

## Follow-up note (not built)

The clean-run gain (+14.5 score, -18% p50) comes from removing per-request CPU
the server still owned (sync dispatch, snapshots, no-copy bodies). What is left
is client-side (undici fetch + the env process's own CPU) and VM scheduling,
neither of which the entry controls — the same wall v3 and v3b hit. Any further
lever would need to cut the *number* of requests per clone (4 per clone, all
client-driven) or the client's per-request cost, neither of which is server-side.
