.PHONY:	all build test shell dialyze

INCLUDE_DIR ?= include
SRC_DIR ?= src
TEST_DIR ?= test
ERLC ?= erlc
ERLC_FLAGS ?= +debug_info +warn_obsolete_guard +warn_export_all -Werror
ERLC_FLAGS += -I $(INCLUDE_DIR)

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

shell:
	erl -pa ebin

dialyze:
	dialyzer -Wunderspecs -Wunmatched_returns -Werror_handling -Wrace_conditions ebin
