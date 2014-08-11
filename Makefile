.PHONY:	all compile check clean conf_clean docs test

all: compile

compile:
	./rebar compile

check:
	@:

clean:
	./rebar clean
	$(RM) doc/*

conf_clean:
	@:

docs:
	./rebar doc

test:
	@:

debug-shell:
	erl -pa ebin

dialyze:
	dialyzer -Wunderspecs -Wunmatched_returns -Werror_handling -Wrace_conditions ebin
