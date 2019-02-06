KAFKA_VERSION ?= 1.1
PROJECT = brod
PROJECT_DESCRIPTION = Kafka client library in Erlang
PROJECT_VERSION = 3.7.5

DEPS = supervisor3 kafka_protocol

ERLC_OPTS = -Werror +warn_unused_vars +warn_shadow_vars +warn_unused_import +warn_obsolete_guard +debug_info -Dbuild_brod_cli

dep_supervisor3_commit = 1.1.8
dep_kafka_protocol_commit = 2.2.7
dep_kafka_protocol = git https://github.com/klarna/kafka_protocol.git $(dep_kafka_protocol_commit)

EDOC_OPTS = preprocess, {macros, [{build_brod_cli, true}]}

## Make app the default target
## To avoid building a release when brod is used as a erlang.mk project's dependency
app::

include erlang.mk

compile:
	@rebar3 compile

test-env:
	@./scripts/setup-test-env.sh

ut:
	@rebar3 eunit -v --cover_export_name ut-$(KAFKA_VERSION)

# version check, eunit and all common tests
t: vsn-check ut
	@rebar3 ct -v --cover_export_name ct-$(KAFKA_VERSION)

vsn-check:
	@./scripts/vsn-check.sh $(PROJECT_VERSION)

distclean:: rebar-clean

rebar-clean:
	@rebar3 clean
	@rm -rf _build
	@rm -rf doc
	@rm -f pipe.testdata

hex-publish: distclean
	@rebar3 hex publish

## tests that require kafka running at localhost
INTEGRATION_CTS = brod_cg_commits brod_client brod_compression brod_consumer brod_producer brod_group_subscriber brod_topic_subscriber

## integration tests
it: ut $(INTEGRATION_CTS:%=it-%)

$(INTEGRATION_CTS:%=it-%):
	@rebar3 ct -v --suite=test/$(subst it-,,$@)_SUITE --cover_export_name $@-$(KAFKA_VERSION)

## build escript and a releas, and copy escript to release bin dir
brod-cli:
	@rebar3 as brod_cli escriptize
	@rebar3 as brod_cli release
	@cp _build/brod_cli/bin/brod_cli _build/brod_cli/rel/brod/bin/

cover:
	@rebar3 cover -v

coveralls:
	@rebar3 coveralls send
