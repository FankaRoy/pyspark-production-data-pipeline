# Environment
PYTHON=python

# Generate synthetic dataset
generate-data:
	$(PYTHON) scripts/generate_synthetic_data.py

# Run Spark pipeline
run-pipeline:
	$(PYTHON) run_pipeline.py

# Full workflow
run: generate-data run-pipeline

# Clean generated output
clean:
	rm -rf data/processed/*
