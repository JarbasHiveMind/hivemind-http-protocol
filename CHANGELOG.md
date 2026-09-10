# Changelog

## [0.0.14a1](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.14a1) (2026-09-10)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.13a1...0.0.14a1)

**Merged pull requests:**

- fix: admit clients without the retired crypto\_key field or the legacy handshake gate [\#52](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/52) ([openvoiceos-bot](https://github.com/openvoiceos-bot))

## [0.0.13a1](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.13a1) (2026-09-10)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.12a1...0.0.13a1)

**Merged pull requests:**

- fix: carry protocol v3 Noise transport frames over HTTP [\#51](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/51) ([openvoiceos-bot](https://github.com/openvoiceos-bot))

## [0.0.12a1](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.12a1) (2026-09-07)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.11a1...0.0.12a1)

**Merged pull requests:**

- fix: tolerate core 5.x dropping handshake flags \(supersedes \#46\) [\#47](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/47) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.11a1](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.11a1) (2026-09-01)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.10a1...0.0.11a1)

**Merged pull requests:**

- fix: accept an optional close code in the disconnect callback \(widened HiveMind contract\) [\#44](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/44) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.10a1](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.10a1) (2026-09-01)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.9a1...0.0.10a1)

**Merged pull requests:**

- fix: refresh the conn\_nonce TTL with the session and make first-connect nonce atomic [\#42](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/42) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.9a1](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.9a1) (2026-09-01)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.8a3...0.0.9a1)

**Merged pull requests:**

- fix: persist conn\_nonce across HTTP replicas [\#40](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/40) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.8a3](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.8a3) (2026-08-31)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.8a2...0.0.8a3)

**Merged pull requests:**

- refactor: drop dead client blacklist copies [\#38](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/38) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.8a2](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.8a2) (2026-08-15)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.8a1...0.0.8a2)

**Merged pull requests:**

- docs: add AGENTS.md with per-repo agent conventions [\#36](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/36) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.8a1](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.8a1) (2026-08-13)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.7a3...0.0.8a1)

**Merged pull requests:**

- fix: carry every ACL field from the database onto an HTTP client [\#34](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/34) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.7a3](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.7a3) (2026-08-10)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.7a2...0.0.7a3)

**Merged pull requests:**

- docs: correct claims that no longer match the code [\#32](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/32) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.7a2](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.7a2) (2026-08-10)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.7a1...0.0.7a2)

**Merged pull requests:**

- chore\(ci\): drop the broken, redundant Dependabot config [\#30](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/30) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.7a1](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.7a1) (2026-08-10)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.6a2...0.0.7a1)

**Merged pull requests:**

- fix: bound the HTTP undelivered-message queues \(TRANSPORT-1 §4\) [\#24](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/24) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.6a2](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.6a2) (2026-08-10)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.6a1...0.0.6a2)

**Merged pull requests:**

- refactor: own the per-client HTTP state in ClientRegistry [\#19](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/19) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.6a1](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.6a1) (2026-08-10)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.5a2...0.0.6a1)

**Merged pull requests:**

- fix: make HTTP session state replica-aware [\#17](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/17) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.5a2](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.5a2) (2026-08-10)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.5a1...0.0.5a2)

**Merged pull requests:**

- refactor: drop dead skill/intent blacklist assignment [\#18](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/18) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.5a1](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.5a1) (2026-08-02)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.4a2...0.0.5a1)

**Merged pull requests:**

- fix: default the HTTP binding to port 5679, not the WebSocket port [\#22](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/22) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.4a2](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.4a2) (2026-07-30)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.4a1...0.0.4a2)

**Merged pull requests:**

- docs: rewrite README in Simplified Technical English [\#20](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/20) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.4a1](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.4a1) (2026-07-04)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.3a1...0.0.4a1)

**Merged pull requests:**

- fix: pin poorman-handshake\>=2.0.0a1 + disable-able runtime password backstop [\#15](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/15) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.3a1](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.3a1) (2026-06-06)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.2a3...0.0.3a1)

**Merged pull requests:**

- fix: drop removed message\_blacklist read \(+ sha256 cert\) [\#13](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/13) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.2a3](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.2a3) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.2a2...0.0.2a3)

**Merged pull requests:**

- docs: zero-to-hero README and /docs coverage [\#10](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/10) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.2a2](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.2a2) (2025-12-19)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.2a1...0.0.2a2)

**Merged pull requests:**

- Update actions/setup-python action to v6 [\#7](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/7) ([renovate[bot]](https://github.com/apps/renovate))

## [0.0.2a1](https://github.com/JarbasHiveMind/hivemind-http-protocol/tree/0.0.2a1) (2025-12-18)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-http-protocol/compare/0.0.1...0.0.2a1)

**Merged pull requests:**

- Configure Renovate [\#2](https://github.com/JarbasHiveMind/hivemind-http-protocol/pull/2) ([renovate[bot]](https://github.com/apps/renovate))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
