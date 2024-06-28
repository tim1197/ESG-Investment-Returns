setup:
	python -m venv venv && \
	source venv/bin/activate && \
	pip install -r requirements.txt && \
	pip install -e .

kernel:
	@if [ -z "$$VIRTUAL_ENV" ]; then \
		echo "Virtual environment is not activated. Activating..."; \
		source venv/bin/activate; \
	fi; \
	pip install ipykernel && \
	python -m ipykernel install --user --name esg-investment-returns

database:
	docker pull postgres && \
	docker run --name esg-investment-returns-db -e POSTGRES_PASSWORD=your_password -d postgres

