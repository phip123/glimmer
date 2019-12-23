.PHONY: usage clean clean-dist docker login  push

VENV_BIN = virtualenv -p python3.7
VENV_DIR ?= .venv

VENV_ACTIVATE = . $(VENV_DIR)/bin/activate

usage:
	@echo "select a build target"

venv: $(VENV_DIR)/bin/activate

$(VENV_DIR)/bin/activate: requirements.txt requirements-dev.txt
	test -d .venv || $(VENV_BIN) .venv
	$(VENV_ACTIVATE); pip install -Ur requirements.txt
	$(VENV_ACTIVATE); pip install -Ur requirements-dev.txt
	touch $(VENV_DIR)/bin/activate

pytest: venv
	$(VENV_ACTIVATE); pytest tests/ --cov controller/

install: venv
	$(VENV_ACTIVATE); python setup.py install

docker:
	docker build -t smart-homie/home-controller .

login:
	docker login hyde.infosys.tuwien.ac.at:5005

push: login docker
	docker tag smart-homie/home-controller hyde.infosys.tuwien.ac.at:5005/iot19/g2b/smart-homie/home-controller
	docker push hyde.infosys.tuwien.ac.at:5005/iot19/g2b/smart-homie/home-controller
