#!/bin/sh
set -e

protoc --go_out=. --go_opt=paths=source_relative task.proto


