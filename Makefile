.ONESHELL:
.PHONY: test run run_coverage report_coverage development-diff-cover uninstall build verify-image-context install setup verify-deploy-image deploy down

DYDX ?= 0
ENV_FILE := setup/environment.yml
# Identity is captured inside the build shell; never expand or export caller-provided values.
unexport SOURCE_COMMIT BUILD_DATE
ifeq ($(DYDX),1)
  ENV_FILE := setup/environment_dydx.yml
endif

test:
	python -m coverage run -m pytest \
	--import-mode=importlib \
 	--ignore="test/mock" \
 	--ignore="test/hummingbot/connector/exchange/ndax/" \
 	--ignore="test/hummingbot/connector/derivative/dydx_v4_perpetual/" \
 	--ignore="test/hummingbot/remote_iface/" \
 	--ignore="test/connector/utilities/oms_connector/" \
 	--ignore="test/hummingbot/strategy/amm_arb/" \
 	--ignore="test/hummingbot/strategy/cross_exchange_market_making/" \

run_coverage: test
	python -m coverage report
	python -m coverage html

report_coverage:
	python -m coverage report
	python -m coverage html

development-diff-cover:
	python -m coverage xml
	diff-cover --compare-branch=origin/development coverage.xml

verify-image-context:
	@if ! git diff --quiet -- || ! git diff --cached --quiet || [ -n "$$(git ls-files --others --exclude-standard)" ]; then \
		echo "Image build requires a clean Git worktree." >&2; \
		exit 1; \
	fi

build: verify-image-context
	@source_commit="$$(git rev-parse --verify HEAD)"; \
	if ! printf '%s\n' "$$source_commit" | grep -Eq '^[0-9a-f]{40}$$'; then \
		echo "Image build requires an exact lowercase Git SHA." >&2; \
		exit 1; \
	fi; \
	build_date="$$(git show -s --format=%cI "$$source_commit")"; \
	branch="$$(git branch --show-current | LC_ALL=C tr -cd '[:alnum:]./_-')"; \
	git archive --format=tar "$$source_commit" | docker build -t "hummingbot/hummingbot$${TAG}" -f Dockerfile \
		--build-arg "SOURCE_COMMIT=$$source_commit" \
		--build-arg "COMMIT=$$source_commit" \
		--build-arg "BRANCH=$$branch" \
		--build-arg "BUILD_DATE=$$build_date" -


uninstall:
	conda env remove -n hummingbot -y

install:
	@if ! command -v conda >/dev/null 2>&1; then \
		echo "Error: Conda is not found in PATH. Please install Conda or add it to your PATH."; \
		exit 1; \
	fi
	@mkdir -p logs
	@echo "Using env file: $(ENV_FILE)"
	@if conda env list | awk '{print $$1}' | grep -qx hummingbot; then \
		conda env update -n hummingbot -f "$(ENV_FILE)"; \
	else \
		conda env create -n hummingbot -f "$(ENV_FILE)"; \
	fi
	@if [ "$$(uname)" = "Darwin" ]; then \
		conda install -n hummingbot -y appnope; \
	fi
	@conda run -n hummingbot conda develop .
	@conda run -n hummingbot python -m pip install --no-deps -r setup/pip_packages.txt > logs/pip_install.log 2>&1
	@conda run -n hummingbot pre-commit install
	@if [ "$$(uname)" = "Linux" ] && command -v dpkg >/dev/null 2>&1; then \
		if ! dpkg -s build-essential >/dev/null 2>&1; then \
			echo "build-essential not found, installing..."; \
			sudo apt-get update && sudo apt-get upgrade -y && sudo apt-get install -y build-essential; \
		fi; \
	fi
	@conda run -n hummingbot --no-capture-output python setup.py build_ext --inplace

run:
	conda run -n hummingbot --no-capture-output ./bin/hummingbot_quickstart.py $(ARGS)

setup:
	@read -r -p "Include Gateway? [y/N] " ans; \
	if [ "$$ans" = "y" ] || [ "$$ans" = "Y" ]; then \
		echo "COMPOSE_PROFILES=gateway" > .compose.env; \
		echo "Gateway will be included."; \
	else \
		echo "COMPOSE_PROFILES=" > .compose.env; \
		echo "Gateway will NOT be included."; \
	fi

verify-deploy-image:
	@image="$$HUMMINGBOT_IMAGE"; \
	line_count="$$(printf '%sX\n' "$$image" | wc -l | tr -d '[:space:]')"; \
	if [ "$$line_count" != 1 ] || ! printf '%s\n' "$$image" | grep -Eq '^(sha256:[0-9a-f]{64}|hummingbot/hummingbot@sha256:[0-9a-f]{64})$$'; then \
		echo "HUMMINGBOT_IMAGE must be an immutable registry digest or local image ID (published repository: hummingbot/hummingbot)." >&2; \
		exit 1; \
	fi

deploy:
	@set -e; compose_profile_args=""; \
	if [ -f ./.compose.env ]; then \
		if printf 'COMPOSE_PROFILES=\n' | cmp -s - ./.compose.env; then \
			:; \
		elif printf 'COMPOSE_PROFILES=gateway\n' | cmp -s - ./.compose.env; then \
			compose_profile_args="--profile gateway"; \
		else \
			echo ".compose.env must contain exactly one COMPOSE_PROFILES entry (empty or gateway); HUMMINGBOT_IMAGE must be supplied through the process environment." >&2; \
			exit 1; \
		fi; \
	fi; \
	unset COMPOSE_FILE COMPOSE_PROFILES COMPOSE_ENV_FILES COMPOSE_PROJECT_NAME; \
	export COMPOSE_DISABLE_ENV_FILE=1; \
	$(MAKE) --no-print-directory verify-deploy-image; \
	docker compose --project-name hummingbot -f ./docker-compose.yml $$compose_profile_args up -d

down:
	@unset COMPOSE_FILE COMPOSE_PROFILES COMPOSE_ENV_FILES COMPOSE_PROJECT_NAME; \
	export COMPOSE_DISABLE_ENV_FILE=1; \
	HUMMINGBOT_IMAGE="sha256:0000000000000000000000000000000000000000000000000000000000000000" \
		docker compose --project-name hummingbot -f ./docker-compose.yml --profile gateway down
