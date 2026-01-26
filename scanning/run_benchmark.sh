#!/bin/bash
set -e

# Transaction Scan Benchmark Runner
# Simplified wrapper that reads from benchmark.config.json

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Read config from benchmark.config.json if it exists
CONFIG_FILE="$SCRIPT_DIR/benchmark.config.json"
if [ -f "$CONFIG_FILE" ]; then
  GRAPHQL_URL=$(jq -r '.connection.graphql_url // "http://127.0.0.1:7001/graphql"' "$CONFIG_FILE")
  LIMIT=$(jq -r '.benchmark.limit // 50' "$CONFIG_FILE")
  NUM_PAGES=$(jq -r '.benchmark.num_pages // 3' "$CONFIG_FILE")
  MIN_FILTERS=$(jq -r '.benchmark.min_filters // 2' "$CONFIG_FILE")
  REQUIRE_BOUNDS=$(jq -r '.benchmark.require_checkpoint_bounds // true' "$CONFIG_FILE")
else
  GRAPHQL_URL="${GRAPHQL_URL:-http://127.0.0.1:7001/graphql}"
  LIMIT="${LIMIT:-50}"
  NUM_PAGES="${NUM_PAGES:-3}"
  MIN_FILTERS="${MIN_FILTERS:-2}"
  REQUIRE_BOUNDS="${REQUIRE_CHECKPOINT_BOUNDS:-true}"
fi

MODE="quick"
PARAMS_FILE="$SCRIPT_DIR/parameters.json"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --mode)
      MODE="$2"
      shift 2
      ;;
    --url)
      GRAPHQL_URL="$2"
      shift 2
      ;;
    --params)
      PARAMS_FILE="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Simplified benchmark runner that reads from benchmark.config.json"
      echo ""
      echo "Options:"
      echo "  --mode MODE     Benchmark mode: quick, full, manual (default: quick)"
      echo "  --url URL       GraphQL endpoint override"
      echo "  --params FILE   Custom parameters file (for manual mode)"
      echo "  --help          Show this help message"
      echo ""
      echo "Modes:"
      echo "  quick   - Run reduced parameter set (~50 queries)"
      echo "  full    - Run all parameter combinations (5000+ queries)"
      echo "  manual  - Use custom parameters file"
      echo ""
      echo "Configuration:"
      echo "  Edit benchmark.config.json to change defaults"
      echo ""
      echo "Examples:"
      echo "  $0                        # Quick mode with config defaults"
      echo "  $0 --mode full            # Full benchmark"
      echo "  $0 --mode manual --params custom.json"
      exit 0
      ;;
    *)
      echo -e "${RED}Error: Unknown option $1${NC}"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         Transaction Scan Benchmark Suite - Bloom Filter Testing           ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Configuration summary
echo -e "${YELLOW}Configuration:${NC}"
echo "  GraphQL URL:    $GRAPHQL_URL"
echo "  Mode:           $MODE"
echo "  Limit:          $LIMIT results per page"
echo "  Pages:          $NUM_PAGES pages"
echo "  Min Filters:    $MIN_FILTERS"
echo "  Require Bounds: $REQUIRE_BOUNDS"
echo ""

# Check if GraphQL server is running
echo -e "${YELLOW}Checking GraphQL server...${NC}"
if ! curl -s -f -X POST "$GRAPHQL_URL" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ chainIdentifier }"}' > /dev/null 2>&1; then
  echo -e "${RED}Error: GraphQL server not responding at $GRAPHQL_URL${NC}"
  exit 1
fi
echo -e "${GREEN}✓ GraphQL server is running${NC}"
echo ""

# Prepare parameters based on mode
case $MODE in
  quick)
    echo -e "${YELLOW}Generating quick test parameters...${NC}"
    cat > /tmp/quick-scan-params.json << 'EOF'
{
  "filter": {
    "function": [
      "0x00b53b0f4174108627fbee72e2498b58d6a2714cded53fac537034c220d26302"
    ],
    "kind": ["PROGRAMMABLE_TX"],
    "sentAddress": [
      "0x02a212de6a9dfa3a69e22387acfbafbb1a9e591bd9d636e7895dcfc8de05f331"
    ],
    "afterCheckpoint": [10000000, 11000000, 12000000],
    "beforeCheckpoint": [10100000, 11100000, 12100000]
  }
}
EOF
    PARAMS_FILE="/tmp/quick-scan-params.json"
    echo -e "${GREEN}✓ Quick mode parameters ready${NC}"
    ;;

  full)
    echo -e "${YELLOW}Running full benchmark suite...${NC}"
    echo -e "${YELLOW}⚠ This will generate 5000+ parameter combinations${NC}"
    read -p "Continue? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      echo "Cancelled."
      exit 0
    fi
    ;;

  manual)
    if [ ! -f "$PARAMS_FILE" ]; then
      echo -e "${RED}Error: Parameters file not found: $PARAMS_FILE${NC}"
      exit 1
    fi
    echo -e "${GREEN}✓ Using custom parameters: $PARAMS_FILE${NC}"
    ;;

  *)
    echo -e "${RED}Error: Unknown mode '$MODE'${NC}"
    exit 1
    ;;
esac

echo ""

TIMESTAMP=$(date +%Y-%m-%d-%H%M%S)
OUTPUT_FILE_RELATIVE="transaction-scan-$TIMESTAMP/queryTransactions-$TIMESTAMP.json"
OUTPUT_FILE="experiments/$OUTPUT_FILE_RELATIVE"

CHECKPOINT_BOUNDS_ARG=""
if [ "$REQUIRE_BOUNDS" = "true" ]; then
  CHECKPOINT_BOUNDS_ARG="--requireCheckpointBounds"
fi

echo -e "${YELLOW}Starting benchmark...${NC}"
echo ""

cd "$ROOT_DIR"
pnpm ts-node cli.ts \
  --suite transactions-scanning \
  --params-file-path "$PARAMS_FILE" \
  --url "$GRAPHQL_URL" \
  --limit "$LIMIT" \
  --numPages "$NUM_PAGES" \
  --minFilters "$MIN_FILTERS" \
  $CHECKPOINT_BOUNDS_ARG \
  --outputFileName "$OUTPUT_FILE_RELATIVE"

BENCHMARK_EXIT_CODE=$?

if [ $BENCHMARK_EXIT_CODE -ne 0 ]; then
  echo ""
  echo -e "${RED}✗ Benchmark failed with exit code $BENCHMARK_EXIT_CODE${NC}"
  exit $BENCHMARK_EXIT_CODE
fi

echo ""
echo -e "${GREEN}✓ Benchmark completed!${NC}"
echo ""

# Summary
TOTAL_QUERIES=$(grep -c '"index":' "$OUTPUT_FILE" 2>/dev/null || echo "0")
COMPLETED=$(grep -c '"COMPLETED"' "$OUTPUT_FILE" 2>/dev/null || echo "0")
TIMED_OUT=$(grep -c '"TIMED OUT"' "$OUTPUT_FILE" 2>/dev/null || echo "0")

echo -e "${BLUE}Summary:${NC}"
echo "  Total Queries: $TOTAL_QUERIES"
echo "  Completed:     $COMPLETED"
echo "  Timed Out:     $TIMED_OUT"
echo "  Output:        $OUTPUT_FILE"
echo ""

echo -e "${YELLOW}Next Steps:${NC}"
echo "  # FPR analysis (TypeScript):"
echo "  pnpm --filter transactions-scanning scan analyze --overfetch"
echo ""
echo "  # Replay queries:"
echo "  pnpm ts-node cli.ts --suite transactions-scanning --params-file-path $OUTPUT_FILE --replay"
echo ""

echo -e "${GREEN}Done!${NC}"
