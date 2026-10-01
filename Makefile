IMAGE ?= oce-sa-scc-controller:dev

.PHONY: test build image manifests
test:
	go test -race ./...
	go vet ./...

build:
	CGO_ENABLED=0 go build -trimpath -o bin/oce-sa-scc-controller ./cmd/oce-sa-scc-controller

image:
	docker build -t $(IMAGE) .

manifests:
	oc kustomize .
