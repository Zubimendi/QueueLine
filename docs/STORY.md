# The story behind QueueLine

*Base material for a LinkedIn post, a Medium article, or interview
talking points — rewrite in your own voice.*

## The short version (LinkedIn post)

Kicked off a personal roadmap of 30 backend projects, two a week, each
one built to teach a specific production pattern instead of being CRUD
with different table names. Project 1 is the piece of infrastructure I
plan to actually reuse for the other 29: **QueueLine**, a job queue built
on Postgres with a REST API, so it's usable from any language, not just
Go.

The interesting engineering problem wasn't "build a queue" — Postgres's
`FOR UPDATE SKIP LOCKED` makes claim-safety across concurrent workers
almost free. The interesting problem was the *second* race condition
almost nobody handles: what happens when a worker claims a job, stalls
past its lease (a GC pause, a network blip, a container throttle), has
its job reclaimed and handed to someone else, and then *wakes up late*
and tries to report success on a job it no longer owns?

The fix is called a fencing token — a fresh, unique lease ID minted on
every claim, required on every completion, checked against the job's
*current* lease before any write is accepted. I wrote an integration
test that deliberately triggers this exact race and proves the stale
worker's completion is rejected while the legitimate one succeeds. That
test is the part of this project I'm actually proud of.

Repo: <your-fork-url>

## The longer version (Medium article)

### Start with the race condition everyone forgets

Most explanations of "how to build a job queue" stop at "use a database
row lock so two workers can't claim the same job." That's necessary, but
it's not sufficient, and the gap between "necessary" and "sufficient" is
exactly where production incidents live.

Here's the race almost every from-scratch job queue misses: a worker
claims a job. Something slow happens — a garbage collection pause, a
brief network partition, the container getting CPU-throttled by its
orchestrator. Long enough that the job's lease expires, and a separate
reaper process (correctly!) decides this job's worker died, and reclaims
it. A second, healthy worker claims it and starts processing. And then —
this is the part that gets missed — the *first* worker's slow operation
finally finishes, and it dutifully reports "I completed this job!"

If nothing stops that report from being accepted, you now have a job
marked "done" that a second worker might still be halfway through
actually doing, and no record that anything went wrong.

### The fix: fencing tokens

The fix is conceptually simple once you see it: every time a job is
claimed, mint a brand new, unique identifier for *that specific claim* —
not the job's ID, a separate token. Every subsequent write for that job
(heartbeat, complete, fail) has to present that exact token, and the
database write only succeeds if it still matches the job's *current*
token. The moment the reaper reclaims the job and a second worker claims
it fresh, a new token exists, and the first worker's stale token becomes
permanently, structurally invalid for any further writes to that job.

This is a real term — "fencing token" — from Martin Kleppmann's writing
on distributed locks (it's in *Designing Data-Intensive Applications*,
which is genuinely one of the best books available for exactly this kind
of production-grade thinking, and I'd recommend it to anyone building
distributed infrastructure like this).

### Proving it, not just building it

The part of this project I'd actually walk an interviewer through isn't
the schema or the API — it's `test/integration/concurrency_test.go`'s
`TestStaleLeaseCannotCompleteAfterReclaim`. It deliberately: claims a job
with an artificially tiny lease, lets it expire, runs the reaper, has a
second call claim the now-freed job, and then asserts the *first*
worker's stale completion attempt is rejected while the second worker's
legitimate one succeeds. Being able to say "I wrote a test that
deliberately triggers the exact race condition fencing tokens exist to
prevent, and proves the fix works" is a much stronger claim than "I
implemented fencing tokens."

### Why build this first, in a 30-project roadmap

Two reasons. First, it's genuinely reusable — every later project in the
roadmap that needs background work (sending notifications, processing
uploads, running a reconciliation job) can depend on this instead of
rebuilding queue logic each time, which is itself a good story ("I built
infrastructure I kept using across 29 other projects"). Second, starting
with the hardest correctness problem (concurrent claim safety plus
worker-failure safety) first, before anything more "product-shaped,"
sets the bar for the rest of the roadmap — every later project has to
meet the same standard this one does.

## Talking points for an interview

1. **Lead with the race condition, not the feature list.** "Most job
   queues handle 'don't let two workers claim the same job.' Fewer
   handle 'don't let a worker that stalled past its lease corrupt a job
   someone else has since picked up' — that's the harder, more
   interesting problem, and it's what this project is actually about."
2. **Explain fencing tokens in your own words**, including why a simple
   "who owns this job" flag isn't enough (it doesn't distinguish between
   the current owner and a previous, stale one).
3. **Point at the specific test** that proves it, not just "it has
   tests" — being able to describe exactly what
   `TestStaleLeaseCannotCompleteAfterReclaim` does is a concrete,
   verifiable claim.
4. **Be ready to explain the throughput tradeoff** of a single-table
   Postgres-backed queue vs. a dedicated broker like Kafka or NATS —
   knowing when Postgres is the right choice and when it stops being one
   is the actual skill, not "always use Postgres" or "always use Kafka."

## Suggested post formats

**Short (LinkedIn/X):**
> Started a 30-project backend roadmap, 2 projects/week, each one built
> to teach a specific production pattern. Project 1: a job queue on
> Postgres that specifically handles the race condition almost everyone
> misses — a worker that stalls past its lease and "wakes up late," and
> almost corrupts a job someone else has since picked up. Fixed with
> fencing tokens, and I wrote a test that deliberately triggers the exact
> race to prove it. Open source: <link>

**Medium article structure:** the naive "just add a lock" version → why
it's not enough (the stale-worker race, told as a short story) → fencing
tokens explained → the test that proves it → why this is project 1 of 30
and what it unlocks for the rest of the roadmap.
