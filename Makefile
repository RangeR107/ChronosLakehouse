# Local commands. CI runs ruff and pytest directly (.github/workflows/ci.yml).

# Spark needs Java 17; Java 23 is also installed on the laptop and must not be used.
JAVA17 := $(shell /usr/libexec/java_home -v 17 2>/dev/null)

.PHONY: truth

# The truth test on the real Jan-Feb 2017 slice, with the spec's defects on (about 12 minutes).
# Needs data/raw/olist and the chronos-db container; missing either fails instead of skipping.
# Record each pass in the current phase's notes ("Truth test, last passed").
truth:
	JAVA_HOME=$(JAVA17) CHRONOS_REQUIRE_OLIST=1 CHRONOS_REQUIRE_POSTGRES=1 \
		.venv/bin/pytest tests/truth -k olist -q -p no:cacheprovider
