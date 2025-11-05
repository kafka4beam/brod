KAFKA_VERSION ?= 4.0
export KAFKA_VERSION

.PHONY: all
all: compile

.PHONY: compile
compile:
	@rebar3 compile

.PHONY: lint
lint:
	@rebar3 lint

.PHONY: test-env
test-env:
	@./scripts/setup-test-env.sh
	@mkdir -p ./test/data/ssl
	@docker cp kafka-1:/localhost-ca-crt.pem ./test/data/ssl/ca.pem
	@docker cp kafka-1:/localhost-client-key.pem ./test/data/ssl/client-key.pem
	@docker cp kafka-1:/localhost-client-crt.pem ./test/data/ssl/client-crt.pem

.PHONY: ut
ut:
	@rebar3 eunit -v --cover_export_name ut-$(KAFKA_VERSION)

.PHONY: ct
ct:
	@rebar3 ct -v --cover_export_name ct-$(KAFKA_VERSION)

# version check, eunit and all common tests
.PHONY: t
t: ut ct

.PHONY: clean
clean:
	@rebar3 clean
	@rm -rf _build
	@rm -rf ebin deps doc
	@rm -f pipe.testdata

.PHONY: hex-publish
hex-publish: clean
	@rebar3 hex publish --repo=hexpm
	@rebar3 hex build

## tests that require kafka running at localhost
INTEGRATION_CTS = brod_cg_commits brod_client brod_compression brod_consumer brod_producer brod_group_subscriber brod_topic_subscriber brod

.PHONY: cover
cover:
	@rebar3 cover -v

.PHONY: dialyzer
dialyzer:
	@rebar3 dialyzer
