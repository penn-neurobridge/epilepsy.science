.PHONY: help test run

SERVICE_NAME  ?= "epilepsy-science"

.DEFAULT: help

help:
	@echo "Make Help for $(SERVICE_NAME)"
	@echo ""
	@echo "make run             - run the processor locally via docker-compose"
	@echo "make clean           - remove all files from the input / output directories"

run:
	docker-compose -f docker-compose.yml down --remove-orphans
	docker-compose -f docker-compose.yml build
	docker-compose -f docker-compose.yml up --exit-code-from epilepsy-science
	@echo "--------------------------------"
	@echo "$(SERVICE_NAME) finished running! Check data/output/ for results."
	@echo "--------------------------------"

clean:
	rm -rf data/input/*
	rm -rf data/output/*
