#Config
SHELL := /bin/bash

PY ?= python3
DOCKER_COMPOSE ?= docker compose

COMPOSE_DEV ?= docker-compose-dev.yaml
COMPOSE_TESTS ?= docker-compose-tests.yaml

#Default Targeet
.DEFAULT_GOAL := compose


# generar el docker compose segun la config global
compose:
	$(PY) generate-compose.py $(COMPOSE_DEV)

#levantar los services
up: compose
	$(DOCKER_COMPOSE) -f $(COMPOSE_DEV) up -d --build

run: up

#down para bajar los services gracefully
down:
	$(DOCKER_COMPOSE) -f $(COMPOSE_DEV) down -t 5

stop: down

# S=<service>, vacio muestra todos
logs:
	$(DOCKER_COMPOSE) -f $(COMPOSE_DEV) logs -f $(S)

#ps lista contenedores del compose
ps:
	$(DOCKER_COMPOSE) -f $(COMPOSE_DEV) ps

#rebuild fuerza build sin cache
rebuild:
	$(DOCKER_COMPOSE) -f $(COMPOSE_DEV) build --no-cache

#restart reincia down + up
restart: down up

#elimiar el docker docker compose
rm-compose:
	@rm -f $(COMPOSE_DEV) && echo ">> Eliminado $(COMPOSE_DEV)"

#TESTS Y VENV

#test-up: levanta RabbitMQ para tests (docker-compose-tests.yaml)
test-up:
	$(DOCKER_COMPOSE) -f $(COMPOSE_TESTS) up -d

#venv: crea venv e instala deps de requirements.txt
venv:
	@test -d venv || $(PY) -m venv venv
	@venv/bin/pip install -U pip
	@venv/bin/pip install -r requirements.txt

#test: corre pytest -v tests/ dentro del venv: necesita el tests up
test: venv
	venv/bin/pytest -v

#test-down: detiene el contenedor rabbit de tests
test-down:
	$(DOCKER_COMPOSE) -f $(COMPOSE_TESTS) stop -t 1
