PROJECT = brod
PROJECT_VERSION = 2.0-dev

DEPS = supervisor3
TEST_DEPS = meck proper

COVER = 1

EUNIT_OPTS = verbose
ERLC_OPTS = -Werror +warn_unused_vars +warn_shadow_vars +warn_unused_import +warn_obsolete_guard
CT_OPTS = -ct_use_short_names true

include erlang.mk

ERL_LIBS := $(ERL_LIBS):$(CURDIR)

