PROJECT = brod
PROJECT_DESCRIPTION = Kafka client library in Erlang
PROJECT_VERSION = 3.6.0

DEPS = supervisor3 kafka_protocol
TEST_DEPS = docopt jsone meck proper
REL_DEPS = docopt jsone

COMMON_ERLC_OPTS = -Werror +warn_unused_vars +warn_shadow_vars +warn_unused_import +warn_obsolete_guard +debug_info -Dbuild_brod_cli
ERLC_OPTS = $(COMMON_ERLC_OPTS)
TEST_ERLC_OPTS = $(COMMON_ERLC_OPTS)


dep_supervisor3_commit = 1.1.5
dep_kafka_protocol_commit = 2.1.0
dep_kafka_protocol = git https://github.com/klarna/kafka_protocol.git $(dep_kafka_protocol_commit)
dep_docopt = git https://github.com/zmstone/docopt-erl.git 0.1.3

ERTS_VSN = $(shell erl -noshell -eval 'io:put_chars(erlang:system_info(version)), halt()')
ESCRIPT_FILE = scripts/brod_cli
ESCRIPT_EMU_ARGS = -sname brod_cli

EDOC_OPTS = preprocess, {macros, [{build_brod_cli, true}]}
COVER = true

EUNIT_OPTS = verbose

CT_OPTS = -ct_use_short_names true

ERL_LIBS := $(ERL_LIBS):$(CURDIR)

## Make app the default target
## To avoid building a release when brod is used as a erlang.mk project's dependency
app::

include erlang.mk

rel:: escript
	@cp scripts/brod _rel/brod/bin/brod
	@cp $(ESCRIPT_FILE) _rel/brod/bin/brod_cli
	@ln -sf erts-$(ERTS_VSN) _rel/brod/erts
	@tar -pczf _rel/brod.tar.gz -C _rel brod

test-env:
	./scripts/setup-test-env.sh 1.1

t: eunit ct
	./scripts/cover-summary.escript eunit.coverdata ct.coverdata

vsn-check:
	$(verbose) ./scripts/vsn-check.sh $(PROJECT_VERSION)

hex-publish: distclean
	$(verbose) rebar3 hex publish

## tests that require kafka running at localhost
INTEGRATION_CTS = brod_cg_commits brod_client brod_compression brod_consumer brod_producer brod_group_subscriber brod_topic_subscriber

## integration tests
.PHONY: it
it: eunit $(INTEGRATION_CTS:%=ct-%)

