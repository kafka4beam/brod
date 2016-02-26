PROJECT = brod
PROJECT_DESCRIPTION = Kafka client library in Erlang
PROJECT_VERSION = 2.0-dev

DEPS = supervisor3 kafka_protocol
TEST_DEPS = meck proper

dep_kafka_protocol = git ssh://git@stash.int.klarna.net:7999/syn/kafka_protocol.git dev

COVER = 1

EUNIT_OPTS = verbose
ERLC_OPTS = -Werror +warn_unused_vars +warn_shadow_vars +warn_unused_import +warn_obsolete_guard +debug_info
CT_OPTS = -ct_use_short_names true

include erlang.mk

ERL_LIBS := $(ERL_LIBS):$(CURDIR)

t: eunit ct
	./scripts/cover-summary.escript eunit.coverdata ct.coverdata

ESCRIPT_FILE = scripts/$(PROJECT)

