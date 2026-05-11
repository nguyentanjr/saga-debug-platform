#!/usr/bin/env bash
# setup.sh - Run once before your first docker-compose up.
# Downloads the OpenTelemetry Java agent (the only file that can't be in Docker image itself
# because it's volume-mounted at runtime).

set -e
cd "$(dirname "$0")"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[setup]${NC} $*"; }
warn()    { echo -e "${YELLOW}[warn]${NC}  $*"; }
die()     { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

# ── 1. Check Docker is running ────────────────────────────────
docker info > /dev/null 2>&1 || die "Docker is not running. Start Docker Desktop / Docker daemon first."
info "Docker OK"

# ── 2. Download OTEL Java agent ───────────────────────────────
OTEL_DIR="./otel"
OTEL_JAR="$OTEL_DIR/opentelemetry-javaagent.jar"
OTEL_URL="https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar"

mkdir -p "$OTEL_DIR"

if [ -f "$OTEL_JAR" ]; then
  info "OTEL agent already exists: $OTEL_JAR ($(du -sh "$OTEL_JAR" | cut -f1))"
else
  info "Downloading OpenTelemetry Java agent..."
  if command -v curl &>/dev/null; then
    curl -fL "$OTEL_URL" -o "$OTEL_JAR"
  elif command -v wget &>/dev/null; then
    wget -q "$OTEL_URL" -O "$OTEL_JAR"
  else
    die "Neither curl nor wget found. Please manually download:\n  $OTEL_URL\n  → save as $OTEL_JAR"
  fi
  info "Downloaded: $(du -sh "$OTEL_JAR" | cut -f1)"
fi

# ── 3. Verify otel-collector config exists ────────────────────
if [ ! -f "./otel/otel-collector-config.yaml" ]; then
  die "Missing: ./otel/otel-collector-config.yaml"
fi
info "otel-collector-config.yaml OK"

# ── 4. Verify postgres-init exists ───────────────────────────
if [ ! -f "./postgres-init/init.sql" ]; then
  die "Missing: ./postgres-init/init.sql"
fi
info "postgres-init/init.sql OK"

echo ""
info "Setup complete. You can now run:"
echo "    docker-compose build          # first time only (takes ~5 min; downloads Maven deps)"
echo "    docker-compose up -d          # start everything"
echo "    ./scripts/wait-ready.sh       # wait until all services are healthy"
echo "    ./scripts/run-scenario.sh happy   # run your first test"
