.PHONY:	all build test docs shell plt dialyze

APP = brod
INCLUDE_DIR ?= include
SRC_DIR ?= src
TEST_DIR ?= test

APP_FILE = $(EBIN_DIR)/$(APP).app
APP_SRC_FILE = $(SRC_DIR)/$(APP).app.src

ERLC ?= erlc
ERLC_FLAGS ?= +debug_info +warn_obsolete_guard +warn_export_all -Werror
ERLC_FLAGS += -I $(INCLUDE_DIR)

DIALYZER_PLT = $(HOME)/.brod_plt
DIALYZER_OPTS = -Werror_handling -Wrace_conditions -Wunderspecs
export DIALYZER_PLT

# needed for edoc
export ERL_LIBS=$(abspath ..)

SOURCES := $(wildcard $(SRC_DIR)/*.erl)
TEST_SOURCES := $(wildcard $(TEST_DIR)/*.erl)
EBIN_DIR ?= ebin
OBJECTS := $(addprefix $(EBIN_DIR)/, $(notdir $(SOURCES:%.erl=%.beam)))
TEST_OBJECTS := $(addprefix $(EBIN_DIR)/, $(notdir $(TEST_SOURCES:%.erl=%.beam)))
MODULES := $(sort $(OBJECTS:$(EBIN_DIR)/%.beam=%))

# comma-separated list of single-quoted module names
# (the comma/space variables are needed to work around Make's argument parsing)
comma := ,
space :=
space +=
MODULES_LIST := $(subst $(space),$(comma)$(space),$(patsubst %,'%',$(MODULES)))

all: build

build: export ERLC_FLAGS := -DNOTEST $(ERLC_FLAGS)
build: $(APP_FILE) $(OBJECTS)

test: $(APP_FILE) $(OBJECTS) $(TEST_OBJECTS)
	erl -noshell -pa $(EBIN_DIR) \
		-eval 'eunit:test("$(EBIN_DIR)", [verbose])' \
		-s init stop

$(APP_FILE): $(APP_SRC_FILE)
	@echo "Writing $(APP_FILE)"
	@sed "s/modules,\[\]/modules, [$(MODULES_LIST)]/" $(APP_SRC_FILE) > $(APP_FILE)

docs: build
	@erl -noshell -eval 'edoc:application($(APP)), init:stop()'

$(EBIN_DIR)/%.beam: $(SRC_DIR)/%.erl
	$(ERLC) $(ERLC_FLAGS) -o $(EBIN_DIR) $<

$(EBIN_DIR)/%.beam: $(TEST_DIR)/%.erl
	$(ERLC) $(ERLC_FLAGS) -o $(EBIN_DIR) $<

clean:
	@rm -f $(OBJECTS)
	@rm -f $(TEST_OBJECTS)
	@rm -f $(DIALYZER_PLT)

shell:
	erl -pa ebin

PLT_APPS = compiler	\
           crypto	\
           dialyzer	\
           erts	\
           eunit	\
           kernel	\
           stdlib	\
           syntax_tools

$(DIALYZER_PLT):
	dialyzer --build_plt --apps $(PLT_APPS) ebin

plt: $(DIALYZER_PLT)

dialyze: $(DIALYZER_PLT)
	dialyzer --no_native --src -r src $(DIALYZER_OPTS)
