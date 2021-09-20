#!/usr/bin/env bash
ipcluster start -n 200 --daemon --profile=asv
asv run
ipcluster stop --profile=asv
