#!/bin/bash

set -e
set -x

golangci-lint run ./...
