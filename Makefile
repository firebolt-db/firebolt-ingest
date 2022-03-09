.PHONY: help build buildx build_dev buildx_dev run_local push

REGISTRY ?= 231290928314.dkr.ecr.us-east-1.amazonaws.com/
REPO ?= firebolt_ingest
TAG ?= latest
DOCKERIMG = "$(REGISTRY)$(REPO):$(TAG)"

SOURCE_MOUNT ?= $(shell pwd)/src/firebolt_ingest
DEST_MOUNT ?= /app/src/firebolt_ingest

help:
	@echo "make build - build production docker image (on host architecture)"
	@echo "make buildx - build docker image on linux/amd64"
	@echo "make build_dev - build development docker image (on host architecture)"
	@echo "make buildx_dev - build development docker image on linux/amd64"
	@echo "make run_local - run development docker container locally"
	@echo "make test - run unit tests"
	@echo "make clean - remove temporary/generated local files"
	@echo "make bash - enter a bash terminal inside the docker container (for development/debugging)"
	@echo "make push - push image to ECR"

build:
	docker build --target prod --tag=$(DOCKERIMG) .

buildx:
	docker buildx build --load --target prod --platform linux/amd64 --tag=$(DOCKERIMG) .

build_dev:
	docker build --target dev --tag=$(DOCKERIMG) .

buildx_dev:
	docker buildx build --load --platform linux/amd64 --target dev --tag=$(DOCKERIMG) .

# note: $$ resolves to $: https://stackoverflow.com/questions/18143805/
clean:
	find . | grep -E "(.egg-info|.ipynb_checkpoints|__pycache__|./typings|\.pyc|\.pyo$$)" | xargs rm -rf

run_local:
	docker run \
		--rm \
		--env-file .env \
		--volume $(HOME)/.aws:/root/.aws:ro \
		--volume $(SOURCE_MOUNT):$(DEST_MOUNT):ro \
		--tty --interactive \
		$(DOCKERIMG)

test:
	docker run \
		--rm \
		--env-file .env \
		--volume $(HOME)/.aws:/root/.aws:ro \
		--volume $(SOURCE_MOUNT):$(DEST_MOUNT):ro \
		--tty --interactive \
		$(DOCKERIMG) pytest

bash:
	docker run \
		--rm \
		--env-file .env \
		--volume $(HOME)/.aws:/root/.aws:ro \
		--tty --interactive \
		$(DOCKERIMG) bash

jupyter:
	docker run \
		-p 8888:8888 \
		--rm \
		--env-file .env \
		--volume $(HOME)/.aws:/root/.aws:ro \
		--volume $(shell pwd)/src:/src:rw \
		--tty --interactive \
		$(DOCKERIMG) \
		jupyter notebook --allow-root --ip=0.0.0.0

push:
	aws --profile publishing ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $(REGISTRY)
	docker push $(DOCKERIMG)
