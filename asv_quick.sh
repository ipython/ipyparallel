#!/usr/bin/env bash
echo "starting engines"
ipcluster start -n 100 --daemon --profile=asv
echo "starting benchmarks"
asv run --quick --show-stderr
echo "Bebchmarks finished"
ipcluster stop --profile=asv