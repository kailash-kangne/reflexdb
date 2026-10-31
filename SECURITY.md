# Security Policy

## Reporting a vulnerability

Do **not** open a public issue for security reports. Instead:

- Open a private report via GitHub's **Security → Report a vulnerability** on this repository, or
- If that's unavailable, contact the maintainer privately (see the repository profile).

Please include: the version/commit tested, a reproduction (config file, `redis-cli`/RESP
transcript, or script), the impact you believe it has, and whether you need the report kept
under embargo. We aim to acknowledge reports within 7 days.

## Scope

ReflexDB is a development-stage RESP-compatible store. Please report anything where a client
can bypass authentication or ACL checks, crash or wedge the server, escape the Lua sandbox,
exhaust memory/disk beyond configured limits, or corrupt persisted data. Threat model and known
gaps: [docs/SECURITY.md](docs/SECURITY.md).
