SHELL := /usr/bin/env bash
GOBIN ?= $(shell go env GOPATH)/bin
PROTOC_GEN_GO_VERSION ?= v1.36.6
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1
COMPOSE ?= docker compose
KIND_CLUSTER ?= inferroute
IMAGE_TAG ?= dev

.PHONY: all proto proto-tools build test test-race vet fmt up down logs bench chaos-bench images kind-up kind-metrics-server kind-load kind-deploy kind-down clean

all: vet test build

proto-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

proto:
	PATH="$(GOBIN):$$PATH" protoc -I proto \
		--go_out=gen/inferencepb --go_opt=paths=source_relative \
		--go-grpc_out=gen/inferencepb --go-grpc_opt=paths=source_relative \
		proto/inference.proto

build:
	CGO_ENABLED=0 go build -trimpath -o bin/router ./cmd/router
	CGO_ENABLED=0 go build -trimpath -o bin/worker ./cmd/worker

vet:
	go vet ./...

fmt:
	gofmt -s -w cmd internal

test:
	go test ./...

test-race:
	go test -race -count=1 ./...

up:
	$(COMPOSE) up -d --build router worker-1 worker-2 worker-3 prometheus grafana

down:
	$(COMPOSE) --profile bench down -v --remove-orphans

logs:
	$(COMPOSE) logs -f router

bench:
	BENCH_UID=$$(id -u) BENCH_GID=$$(id -g) $(COMPOSE) --profile bench run --rm \
		-e SUMMARY_FILE=/bench/results/load-$$(date +%Y%m%d-%H%M%S).json k6 run /bench/load.js

chaos-bench:
	./benchmarks/chaos.sh

images:
	docker build -f Dockerfile.router -t inferroute/router:$(IMAGE_TAG) .
	docker build -f Dockerfile.worker -t inferroute/worker:$(IMAGE_TAG) .

kind-up:
	kind create cluster --name $(KIND_CLUSTER) --config k8s/kind-config.yaml

# The HPA needs metrics-server; Kind's kubelets use self-signed certs.
kind-metrics-server:
	kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml
	kubectl -n kube-system patch deploy metrics-server --type=json \
		-p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'

kind-load: images
	kind load docker-image inferroute/router:$(IMAGE_TAG) inferroute/worker:$(IMAGE_TAG) --name $(KIND_CLUSTER)

kind-deploy: kind-load
	kubectl apply -k k8s/
	kubectl -n inferroute rollout status deploy/inferroute-worker --timeout=120s
	kubectl -n inferroute rollout status deploy/inferroute-router --timeout=120s

kind-down:
	kind delete cluster --name $(KIND_CLUSTER)

clean:
	go clean -testcache
	rm -f bin/router bin/worker
