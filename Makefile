.PHONY:	all build test shell plt dialyze

INCLUDE_DIR ?= include
SRC_DIR ?= src
TEST_DIR ?= test
ERLC ?= erlc
ERLC_FLAGS ?= +debug_info +warn_obsolete_guard +warn_export_all -Werror
ERLC_FLAGS += -I $(INCLUDE_DIR)

DIALYZER_PLT = $(HOME)/.brod_plt
DIALYZER_OPTS = -Werror_handling -Wrace_conditions -Wunderspecs
export DIALYZER_PLT

SOURCES := $(wildcard $(SRC_DIR)/*.erl)
TEST_SOURCES := $(wildcard $(TEST_DIR)/*.erl)
EBIN_DIR ?= ebin
OBJECTS := $(addprefix $(EBIN_DIR)/, $(notdir $(SOURCES:%.erl=%.beam)))
TEST_OBJECTS := $(addprefix $(EBIN_DIR)/, $(notdir $(TEST_SOURCES:%.erl=%.beam)))

all: build

build: $(OBJECTS)

test: export ERLC_FLAGS := -DTEST $(ERLC_FLAGS)
test: $(OBJECTS) $(TEST_OBJECTS)
	erl -noshell -pa $(EBIN_DIR) \
		-eval 'eunit:test("$(EBIN_DIR)", [verbose])' \
		-s init stop

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
