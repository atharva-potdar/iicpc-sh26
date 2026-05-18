# CLAUDE.md
This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands
- **Local Dev Deploy**: `just` (rebuilds, loads into k0s, deploys via Helm, runs smoke test)
- **Tear Down**: `just dev-teardown`
- **Clean Cache**: `just clean-cache`
- **Lint Helm Charts**: `just helm-lint`
- **Port Forwarding (Required for testing)**:
  - Submission API: `kubectl port-forward -n platform svc/submission-api 8080:8080`
  - SeaweedFS: `kubectl port-forward -n platform svc/seaweedfs 8333:8333`
  - Leaderboard UI: `kubectl port-forward -n platform svc/leaderboard-ws 8090:8090`
- **E2E Smoke Test Submission** (Requires 8080 & 8333 forwarded):
  ```bash
  RESP=$(curl -s -X POST http://localhost:8080/submissions -H "Content-Type: application/json" -d '{"language":"go","team_name":"test-team"}') && URL=$(echo $RESP | jq -r '.presigned_url') && ID=$(echo $RESP | jq -r '.submission_id') && curl -X PUT -T ~/Projects/testserver.tar.gz "$URL" --resolve "seaweedfs.platform.svc.cluster.local:8333:127.0.0.1" && curl -s -X POST http://localhost:8080/submissions/$ID/confirm
  ```

## Development & Troubleshooting
- **Watch Pod Status**: `watch -n 1 kubectl get pods -A`
- **Check Logs**:
  - Build Service: `kubectl logs -n platform deployment/build-service | tail -30`
  - Bot Runner: `kubectl logs -n bots <bot-runner-pod-name> -f`
- **Inspect Sandboxes**: `kubectl describe pod <sandbox-pod-name> -n sandboxes`

## Code Style & Conventions
- **Language**: Go 1.26 monorepo under `services/`.
- **Logging**: Use standard library structured logging (`slog`) across all services.
- **Architecture**: Services must communicate exclusively via Redpanda (Franz-go library) using events; avoid direct HTTP communication between internal platform services.
- **Commit Style**: Use semantic commits including the scope/domain (e.g., `fix(bot-runner): message` or `feat(submission-api): message`). The commit body must consist of an unordered list using hyphens (`-`) and newlines for a concise listing of changes.

## Communication & Response Rules
- **Tool Updates**: State in one concise sentence what you are about to do before starting tool calls. Give brief, one-sentence updates at key moments (e.g., when finding a file, changing direction, or hitting a blocker).
- **Thinking Process**: State results, decisions, and outcomes directly. Do not narrate your internal deliberations or provide running commentaries on your thought process.
- **Summaries**: Keep end-of-turn summaries extremely brief (one or two sentences), describing exactly what changed and what's next.
- **Format Match**: Match response styling and length to the complexity of the task; a simple question must receive a simple, direct answer.
- **Code Comments**: Do not add unnecessary comments. Use a maximum of one short line for critical comments/docstrings; never write multi-paragraph docstrings or multi-line comment blocks.
- **Planning & Analysis**: Work directly from conversation context; do not create planning, decision, or analysis files in the codebase unless explicitly requested by the user.
