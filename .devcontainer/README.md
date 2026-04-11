# Dev Container Usage

This dev container provides a complete development environment with Python 3.12 and pixi.

## Prerequisites

- Docker installed and running
- Node.js (for the devcontainer CLI)
- Claude Code installed on the host at `~/.local/bin/claude`

## Host Mounts

The container mounts these directories from the host:
- `/opt/certs` - SSL certificates (readonly)
- `~/.claude` - Claude Code configuration
- `~/.local/bin` - Local binaries including Claude (readonly)
- `~/.local/share/claude` - Claude Code installation (readonly)

## Quick Start

```bash
# Build and start the container
npx @devcontainers/cli up --workspace-folder .

# Get a shell inside the container
npx @devcontainers/cli exec --workspace-folder . bash
```

## Commands

### Start the Container

```bash
npx @devcontainers/cli up --workspace-folder .
```

### Open a Shell

```bash
npx @devcontainers/cli exec --workspace-folder . bash
```

### Run Claude Code

```bash
# From inside the container shell
claude --dangerously-skip-permissions

# Or directly (use bash -lc to get proper PATH)
npx @devcontainers/cli exec --workspace-folder . bash -lc "claude --dangerously-skip-permissions"
```

### Start the Dev Server

```bash
# Inside the container - starts on port 8100 with SSL
pixi run dev-launch-remote

# For local-only access (no SSL)
pixi run dev-launch
```

Test from host:
```bash
curl -k https://localhost:8100
```

### Stop the Container

```bash
# Find the container ID
docker ps | grep x2s3

# Stop it
docker stop <container_id>
```

### Rebuild from Scratch

Use this after modifying Dockerfile or devcontainer.json:

```bash
npx @devcontainers/cli up --workspace-folder . --remove-existing-container
```

## Inside the Container

| Command | Description |
|---------|-------------|
| `pixi run dev-install` | Install package in editable mode |
| `pixi run dev-launch` | Start dev server on port 8100 (localhost only) |
| `pixi run dev-launch-remote` | Start dev server on port 8100 with SSL (accessible remotely) |
| `pixi run test` | Run tests with coverage |
| `claude` | Claude Code CLI |

## Port Configuration

The container exposes port 8100 on all interfaces (`0.0.0.0:8100:8100`), allowing remote access when using `dev-launch-remote`.

## VS Code / Cursor

You can also open the project in VS Code or Cursor and use the "Reopen in Container" command for a GUI-based experience.

**Note:** Remote SSH + Dev Containers in Cursor has known issues. The CLI approach is more reliable for remote development.
