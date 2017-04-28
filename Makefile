PROJECT = brod
PROJECT_DESCRIPTION = Kafka client library in Erlang
PROJECT_VERSION = 2.3.7

DEPS = supervisor3 kafka_protocol

ERLC_OPTS = -Werror +warn_unused_vars +warn_shadow_vars +warn_unused_import +warn_obsolete_guard +debug_info
TEST_ERLC_OPTS = -Werror +warn_unused_vars +warn_shadow_vars +warn_unused_import +warn_obsolete_guard +debug_info

ifeq ($(BROD_CLI),true)
	DEPS += docopt jsone
	ERLC_OPTS += -DBROD_CLI
	TEST_ERLC_OPTS += -DBROD_CLI
endif

dep_supervisor3_commit = 1.1.5
dep_kafka_protocol_commit = 0.9.1

dep_docopt = git https://github.com/zmstone/docopt-erl.git 0.1.3

ESCRIPT_FILE = scripts/brod

TEST_DEPS = meck proper

COVER = true

EUNIT_OPTS = verbose

CT_OPTS = -ct_use_short_names true

include erlang.mk

rel:: escript
	cp $(ESCRIPT_FILE) _rel/brod/bin/brod
	tar -pczf _rel/brod.tar.gz -C _rel brod

ERL_LIBS := $(ERL_LIBS):$(CURDIR)

test-env:
	./scripts/setup-test-env.sh

t: eunit ct
	./scripts/cover-summary.escript eunit.coverdata ct.coverdata

vsn-check:
	$(verbose) ./scripts/vsn-check.sh $(PROJECT_VERSION)

hex-publish: distclean
	$(verbose) rebar3 hex publish

