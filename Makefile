all: build

build:
	@jbuilder build @install

clean:
	@jbuilder clean

install: build
	@jbuilder install

test: install
	@cd test && jbuilder build test.exe

run-test: test
	@cd test && _build/default/test.exe

clean-test:
	@cd test && jbuilder clean
