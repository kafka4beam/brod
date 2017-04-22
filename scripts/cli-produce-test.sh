#!/bin/bash -e

THIS_DIR="$(dirname $(readlink -f $0))"

BROD=$THIS_DIR/brod

$BROD send -b localhost -t test-topic -p 0 -k $(date +%s) -v $(date +%y-%m-%d-%H-%M-%S)

