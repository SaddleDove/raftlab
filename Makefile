.PHONY: all test vet staticcheck race importcheck gate-m0 ci tools

GO ?= go
PKGS := ./...

all: test

tools:
	$(GO) install honnef.co/go/tools/cmd/staticcheck@v0.6.1

test:
	$(GO) test $(PKGS)

vet:
	$(GO) vet $(PKGS)

importcheck:
	$(GO) run ./cmd/importcheck

staticcheck: tools
	staticcheck $(PKGS)

race:
	$(GO) test -race $(PKGS)

gate-m0:
	$(GO) run ./cmd/raftlab gate-m0 -seeds 3 -until 200

gate-m1:
	$(GO) run ./cmd/raftlab gate-m1 -seeds 1000

gate-m2:
	$(GO) run ./cmd/raftlab gate-m2

gate-m3:
	$(GO) run ./cmd/raftlab gate-m3 -seeds 1000

gate-m4:
	$(GO) run ./cmd/raftlab gate-m4

gate-m5:
	$(GO) run ./cmd/raftlab gate-m5 -seeds 200

gate-m6:
	$(GO) run ./cmd/raftlab gate-m6

gate-m7:
	$(GO) run ./cmd/raftlab gate-m7

gate-m8:
	$(GO) run ./cmd/raftlab gate-m8

ci: importcheck vet test gate-m0
