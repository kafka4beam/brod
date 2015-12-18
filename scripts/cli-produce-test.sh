#!/bin/bash -e

THIS_DIR="$(dirname $(readlink -f $0))"

BROD=$THIS_DIR/brod

$BROD produce localhost $BROD_CLI_PRODUCE_TEST_TOPIC 0 $(date +%s):$(date +%y-%m-%d-%H-%M-%S)

