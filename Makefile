# zfs-csi — build/test/codegen Makefile
BINARY      ?= zfs-csi
IMAGE       ?= ghcr.io/randomvariable/zfs-csi:dev
PLATFORMS   ?= linux/amd64,linux/arm64
CONTROLLER_GEN ?= $(shell which controller-gen)
CRD_OPTIONS  ?= crd:crdVersions=v1,generateEmbeddedObjectMeta=true paths=./api/... output:crd:artifacts:config=deploy/crd output: deepcopy:artifacts:config=-

.PHONY: all build test vet fmt tidy generate crd gosec

all: build test

## build: compile all packages (cgo libzfs path excluded locally via build tag)
build:
	CGO_ENABLED=0 go build ./...

## build-storage: compile including cgo libzfs binding (requires libzfs dev headers/libs)
build-storage:
	CGO_ENABLED=1 go build -tags libzfs ./...

## test: unit + property tests (no cgo, no cluster)
test:
	CGO_ENABLED=0 go test ./...

test-race:
	CGO_ENABLED=0 go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

## generate: deepcopy + CRDs (SSA apply-configs dropped: controller uses Create/Update/Status.Patch)
generate:
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./api/..."
	$(CONTROLLER_GEN) $(CRD_OPTIONS)

crd: generate

## sanity: run csi-sanity against a running driver endpoint (SANITY_ENDPOINT=...)
sanity:
	go test -tags=sanity -v ./test/sanity/...

## docker: multi-arch image (requires buildx)
docker:
	docker buildx build --platform $(PLATFORMS) -t $(IMAGE) --push .
