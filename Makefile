.PHONY:	all compile docs check test clean plt dialyze

REBAR ?= $(shell which rebar 2> /dev/null || which ./rebar)
DIALYZER_PLT = ./brod.plt
DIALYZER_OPTS = --fullpath --no_native -Werror_handling -Wrace_conditions -Wunderspecs -Wno_opaque -Wno_return -Wno_match -Wno_unused --plt $(DIALYZER_PLT)
PLT_APPS = erts kernel stdlib

all: compile

compile:
	@$(REBAR) compile

docs:
	@$(REBAR) skip_deps=true doc

check: compile plt dialyze

get-deps:
	$(REBAR) get-deps

test: REBAR := BROD_TEST=1 $(REBAR)
test: clean get-deps compile
	$(REBAR) eunit -v apps=brod

clean:
	@$(RM) -rf deps
	@$(REBAR) clean
	@$(RM) doc/*
	@$(RM) -f $(DIALYZER_PLT)

plt: $(DIALYZER_PLT)

$(DIALYZER_PLT):
	dialyzer --build_plt --apps $(PLT_APPS) ebin --output_plt $(DIALYZER_PLT)

dialyze: $(DIALYZER_PLT)
	dialyzer -r ebin $(DIALYZER_OPTS)

# eof
