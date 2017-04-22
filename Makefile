PROJECT = brod
PROJECT_DESCRIPTION = Kafka client library in Erlang
PROJECT_VERSION = 2.3.6

DEPS = supervisor3 kafka_protocol

## BROD_CLI=true make escript
ifeq ($(BROD_CLI),true)
	DEPS += docopt jsone
	ERLC_OPTS += -DBROD_CLI
	TEST_ERLC_OPTS += -DBROD_CLI
endif

dep_supervisor3_commit = 1.1.5
dep_kafka_protocol_commit = 0.9.1

dep_docopt = git https://github.com/zmstone/docopt-erl.git 0.1.2

TEST_DEPS = meck proper

COVER = true

EUNIT_OPTS = verbose
ERLC_OPTS += -Werror +warn_unused_vars +warn_shadow_vars +warn_unused_import +warn_obsolete_guard +debug_info
CT_OPTS = -ct_use_short_names true

include erlang.mk

ERL_LIBS := $(ERL_LIBS):$(CURDIR)

test-env:
	./scripts/setup-test-env.sh

t: eunit ct
	./scripts/cover-summary.escript eunit.coverdata ct.coverdata

vsn-check:
	$(verbose) ./scripts/vsn-check.sh $(PROJECT_VERSION)

ESCRIPT_FILE = scripts/$(PROJECT)

hex-publish: distclean
	$(verbose) rebar3 hex publish

