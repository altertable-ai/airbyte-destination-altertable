lint:
	ruff check .
	ruff format .

build:
	docker build -t altertable-ai/destination-altertable .