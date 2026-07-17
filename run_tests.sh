#!/bin/bash
set -e

echo "=== Running Database Unit Tests ==="
node --test test/test_queuectl.js

echo ""
echo "=== Running CLI Integration Tests ==="
node verify_flows.js
