# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- uv.lock lockfile.
- Dependabot config for uv and GitHub Actions (weekly).
- .env.example listing the environment variables SwarmBus reads.
- SECURITY.md with private reporting contact.
- This changelog.
- Optional API-key auth for `/v1/bus/*` via `SWARMBUS_API_KEY` (X-API-Key or Bearer).
- `MAX_HISTORY_LENGTH` cap (default 256) on broadcast history.
- Dead-letter stream `swarmbus:dlq:<topic>` for malformed stream entries.

### Fixed
- Cycle detection is iterative; long acyclic histories no longer raise RecursionError (HTTP 500).
- Off-by-one: a progressing cycle of exactly `MAX_LOOP_DEPTH` agents is now allowed.
- All cycles in a history are evaluated, not only the first one found.
- Consumers no longer crash-loop on a malformed pending stream entry; it is acked and dead-lettered.
