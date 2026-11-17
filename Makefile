GO ?= go
IMAGE ?= openshell/kube-ingress-controller:poc
GOARCH ?= amd64

.PHONY: check test build image
check:
	test -z "$$($(GO) fmt ./...)"
	$(GO) vet ./...
	$(GO) test -race ./...
test:
	$(GO) test -race ./...
build:
	mkdir -p bin
	$(GO) build -o bin/controller .
image:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=$(GOARCH) $(GO) build -trimpath -o bin/controller-linux .
	docker build --platform linux/$(GOARCH) -t $(IMAGE) .
