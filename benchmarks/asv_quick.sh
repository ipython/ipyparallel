#!/usr/bin/env bash
echo "starting engines"
#ipcluster start -n 200 --daemon --profile=asv
echo "starting benchmarks"
asv run --quick --show-stderr
echo "Benchmarks finished"
#ipcluster stop --profile=asv
