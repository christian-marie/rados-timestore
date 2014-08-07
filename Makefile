all:
	cabal build

SOURCES=$(shell find {lib,tests} -name '*.hs' -type f)

HOTHASKTAGS=$(shell which hothasktags 2>/dev/null)
CTAGS=$(if $(HOTHASKTAGS),$(HOTHASKTAGS),/bin/false)

tags: $(SOURCES)
	if [ "$(HOTHASKTAGS)" ] ; then /bin/echo -e "CTAGS\ttags" ; fi
	-$(CTAGS) $^ > tags $(REDIRECT)

format: $(SOURCES)
	stylish-haskell -i $^

lint: $(SOURCES)
	for i in $^; do hlint $$i; done
