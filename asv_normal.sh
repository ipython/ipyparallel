#!/usr/bin/env bash
ipcluster start -n 100 --daemon --profile=asv
asv run
ipcluster stop --profile=asv