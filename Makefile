LOCAL_BIN:=$(CURDIR)/bin

##################### GOLANG-CI RELATED CHECKS #####################
# Check global GOLANGCI-LINT
GOLANGCI_BIN:=$(LOCAL_BIN)/golangci-lint
GOLANGCI_TAG:=1.49.0

# Check local bin version
ifneq ($(wildcard $(GOLANGCI_BIN)),)
GOLANGCI_BIN_VERSION:=$(shell $(GOLANGCI_BIN) --version)
ifneq ($(GOLANGCI_BIN_VERSION),)
GOLANGCI_BIN_VERSION_SHORT:=$(shell echo "$(GOLANGCI_BIN_VERSION)" | sed -E 's/.* version (.*) built from .* on .*/\1/g')
else
GOLANGCI_BIN_VERSION_SHORT:=0
endif
ifneq "$(GOLANGCI_TAG)" "$(word 1, $(sort $(GOLANGCI_TAG) $(GOLANGCI_BIN_VERSION_SHORT)))"
GOLANGCI_BIN:=
endif
endif

# Check global bin version
ifneq (, $(shell which golangci-lint))
GOLANGCI_VERSION:=$(shell golangci-lint --version 2> /dev/null )
ifneq ($(GOLANGCI_VERSION),)
GOLANGCI_VERSION_SHORT:=$(shell echo "$(GOLANGCI_VERSION)"|sed -E 's/.* version (.*) built from .* on .*/\1/g')
else
GOLANGCI_VERSION_SHORT:=0
endif
ifeq "$(GOLANGCI_TAG)" "$(word 1, $(sort $(GOLANGCI_TAG) $(GOLANGCI_VERSION_SHORT)))"
GOLANGCI_BIN:=$(shell which golangci-lint)
endif
endif
##################### GOLANG-CI RELATED CHECKS #####################

# install golangci-lint binary
.PHONY: install-lint
install-lint:
ifeq ($(wildcard $(GOLANGCI_BIN)),)
	$(info #Downloading golangci-lint v$(GOLANGCI_TAG))
	GOBIN=$(LOCAL_BIN) go install github.com/golangci/golangci-lint/cmd/golangci-lint@v$(GOLANGCI_TAG)
GOLANGCI_BIN:=$(LOCAL_BIN)/golangci-lint
endif

# run full lint like in pipeline
.PHONY: .lint
.lint: install-lint
	$(GOLANGCI_BIN) run --config=.golangci.pipeline.yaml ./...

# golangci-lint full
.PHONY: lint
lint: .lint

# Set DATABASE_DSN env var before running. DSN should have keyword/value form,
# see "Connection Strings" section at
# https://www.postgresql.org/docs/current/libpq-connect.html
test:
	$(info #Running tests...)
	go test -v -race -covermode atomic ./...

mock:
	minimock -i github.com/kasorse/pgqueue.storage -o .
	minimock -i github.com/kasorse/pgqueue.workerMonitor -o .
	minimock -i github.com/kasorse/pgqueue.TaskHandler -o .
	minimock -i github.com/kasorse/pgqueue.MetricsCollector -o .

db-status:
	goose -dir migrations postgres "${DATABASE_DSN}" status

db-up:
	goose -dir migrations postgres "${DATABASE_DSN}" up

db-up-by-one:
	goose -dir migrations postgres "${DATABASE_DSN}" up-by-one

db-down:
	goose -dir migrations postgres "${DATABASE_DSN}" down

# Creates migration, run as 'make db-create NAME='<migration name>'
db-create:
	goose -dir migrations -s create "${NAME}" sql
