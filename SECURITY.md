# Security policy

## Reporting a vulnerability

Please **do not open a public issue** for anything you believe is a security
problem. Instead use GitHub's private vulnerability reporting on this
repository (Security → Report a vulnerability). You will get an
acknowledgement within **72 hours** and a status update at least every
**14 days** until resolution.

If private reporting is unavailable to you, email the maintainer address on
the repository profile with `[shm-actors security]` in the subject.

## What counts

This project's security surface is unusual and worth naming precisely:

- **Untrusted bytes.** Every parser that reads attacker-controllable bytes
  (manifests, chain walks, Arrow layout, the wire protocol) is fuzzed and must
  never panic, over-read, or loop. A reproducer that breaks that contract is a
  security bug even without a demonstrated exploit.
- **Shared-memory blast radius.** A process that can map a segment can, today,
  corrupt it for every peer (the threat model and its mitigations — read-only
  consumer maps, sealing, per-tenant capabilities — are tracked publicly on
  the roadmap). Reports that *cross* an intended boundary (e.g. corrupting
  state you were only granted read access to, once that ships) are in scope;
  "a process with write access can write" is the documented status quo, not a
  vulnerability.
- **Crash-reclamation correctness.** Anything that makes journal replay free
  memory still in use (use-after-free across processes) is the highest
  severity class this project has.

## Supported versions

Pre-1.0: only the latest release (and `main`) receives fixes.
