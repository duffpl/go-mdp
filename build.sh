#!/usr/bin/env bash
GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o build/go-mdp_linux_arm64_v2
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o build/go-mdp_linux_amd64_v2