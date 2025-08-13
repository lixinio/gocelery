.PHONY: build
build:
	go install .

.PHONY: lint
lint:
	golangci-lint run

.PHONY: test
test:
	go test -timeout 30s -v -cover .

.PHONY: initpy
initpy:
	export PIPENV_IGNORE_VIRTUALENVS=1
	export PIPENV_VENV_IN_PROJECT=1
	pipenv --python=3.7
	. .venv/bin/activate

.PHONY: worker
worker:
	cd example && \
	celery -A worker worker --loglevel=debug --concurrency=1 \
		--without-heartbeat --without-mingle --without-gossip \
		--broker=redis://localhost:6379/0 \
		--result-backend=redis://localhost:6379/0 && \
	cd -

.PHONY: worker-rbt
worker-rbt:
	cd example && \
	celery -A worker worker --loglevel=debug --concurrency=1 \
		--without-heartbeat --without-mingle --without-gossip \
		--broker=amqp://guest:guest@localhost:5672/ \
		--result-backend=amqp://guest:guest@localhost:5672/ && \
	cd -




