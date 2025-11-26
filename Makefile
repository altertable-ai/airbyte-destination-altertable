lint:
	ruff format .
	ruff check --fix .

build:
	docker build -t altertable-ai/destination-altertable .
