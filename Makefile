SHELL := /bin/bash
PYTHON := python
PIP := pip

BUILD_DIR := ./build
DIST_DIR := ./dist

clean:
	find . -name "*.py[co]" -delete
	rm -f .coverage

distclean: clean
	rm -rf $(BUILD_DIR)
	rm -rf $(DIST_DIR)

deps: py_deploy_deps py_dev_deps

py_deploy_deps: $(BUILD_DIR)/pip-deploy.log

$(BUILD_DIR)/pip-deploy.log: requirements.txt
	@mkdir -p $(BUILD_DIR)
	$(PIP) install -Ur $< && touch $@

py_dev_deps: $(BUILD_DIR)/pip-dev.log

$(BUILD_DIR)/pip-dev.log: requirements_dev.txt
	@mkdir -p $(BUILD_DIR)
	$(PIP) install -Ur $< && touch $@

unit: clean
	nosetests --with-coverage -A 'not integration'

integrations:
	nosetests --with-coverage --logging-level=ERROR -A 'integration'

testall: clean
	nosetests --with-coverage

test: clean unit integrations

release:
	python setup.py sdist bdist_wininst upload -r pypi
