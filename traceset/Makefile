.PHONY: e2e generate ingest validate health test test-unit clean

e2e:
	python3 -m traceset.pipeline

generate:
	python3 -m traceset.generate_traceset

ingest:
	python3 -m traceset.ingest

validate:
	python3 -m traceset.validate

health:
	curl -s http://localhost:3000/api/public/health

test:
	python3 -m pytest traceset/ -v

test-unit:
	python3 -m pytest traceset/ -v -m "not e2e"

clean:
	python3 -m traceset.pipeline --clean
