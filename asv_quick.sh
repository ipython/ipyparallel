#!/usr/bin/env bash
ipcluster start -n 100&
asv run --quick --show-stderr
