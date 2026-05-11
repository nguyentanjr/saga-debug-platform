#!/usr/bin/env bash
# wait-ready.sh - Poll all Spring Boot /actuator/health endpoints.
# Call this after `docker-compose up -d` before running any scenario.
# Services take 30-90s due to: JVM startup + OTEL agent init + Flyway migrations.

set -e
cd "$(dirname "$0")/.."

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

SERVICES=(
  "order-service|http://localhost:8081/actuator/health"
  "payment-service|http://localhost:8082/actuator/health"
  "inventory-service|http://localhost:8083/actuator/health"
  "correlation-service|http://localhost:8084/actuator/health"
)

MAX_WAIT=180  # seconds
POLL=5

echo -e "${YELLOW}Waiting for all services to be healthy (max ${MAX_WAIT}s)...${NC}"

all_up() {
  for entry in "${SERVICES[@]}"; do
    name="${entry%%|*}"
    url="${entry##*|}"
    status=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 2 "$url" 2>/dev/null || echo "000")
    if [ "$status" != "200" ]; then
      return 1
    fi
  done
  return 0
}

elapsed=0
while ! all_up; do
  if [ $elapsed -ge $MAX_WAIT ]; then
    echo -e "${RED}Timed out after ${MAX_WAIT}s. Check logs:${NC}"
    echo "  docker-compose logs order-service"
    echo "  docker-compose logs correlation-service"
    exit 1
  fi
  # Show individual status
  echo -n "  ["
  for entry in "${SERVICES[@]}"; do
    name="${entry%%|*}"
    url="${entry##*|}"
    status=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 2 "$url" 2>/dev/null || echo "000")
    if [ "$status" = "200" ]; then
      echo -n " ${GREEN}${name}✓${NC}"
    else
      echo -n " ${YELLOW}${name}…${NC}"
    fi
  done
  echo " ] ${elapsed}s"
  sleep $POLL
  elapsed=$((elapsed + POLL))
done

echo -e "${GREEN}All services are healthy!${NC}"
echo ""
echo "Endpoints:"
echo "  Order service:       http://localhost:8081"
echo "  Payment service:     http://localhost:8082"
echo "  Inventory service:   http://localhost:8083"
echo "  Correlation service: http://localhost:8084"
echo "  Kafka UI:            http://localhost:8088"
echo "  Jaeger UI:           http://localhost:16686"
