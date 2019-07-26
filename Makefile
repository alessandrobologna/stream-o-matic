STAGE ?= dev
PROJECT ?= stream-o-matic
DB_NAME ?= s3
REGION ?= $(shell aws configure get region)
CODE_BUCKET ?= "$(shell aws sts get-caller-identity --query "Account" --output text).$(PROJECT).code.$(STAGE)"
PYTHON_VERSION ?= python3.7

.PHONY: help runtime

help:
	@echo "Commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo


clean:  ## removes build artifacts
	@rm -rf .aws-sam  runtime/python

bucket:  ## creates the bucket for the lambda code
	@aws s3api create-bucket --bucket $(CODE_BUCKET) --region $(REGION) >/dev/null \
		&& printf "> \033[36mBucket s3://$(CODE_BUCKET) created\033[0m\n"

runtime: ## build the lambda runtime layer
	@printf "> \033[36mBuilding Runtime Layer...\033[0m\n"
	@PY_DIR=runtime/python/lib/$(PYTHON_VERSION)/site-packages &&\
	rm -rf $$PY_DIR && mkdir -p $$PY_DIR &&\
	pip install -r runtime/runtime-reqs.txt -t $$PY_DIR --no-warn-conflicts --ignore-installed --quiet
	@printf "> \033[36mCompleted\033[0m\n"

build: ## run sam build
	@printf "> \033[36mBuilding Application...\033[0m\n"
	@sam build --parameter-overrides ParameterKey=PythonVersion,ParameterValue=$(PYTHON_VERSION) >/dev/null
	@printf "> \033[36mCompleted\033[0m\n"

package: ## run sam package
	@printf "> \033[36mPackaging Application...\033[0m\n"
	@sam package --s3-bucket $(CODE_BUCKET) --output-template-file .aws-sam/build/packaged.yaml >/dev/null	
	@printf "> \033[36mCompleted\033[0m\n"

deploy: ## run sam deploy
	@printf "> \033[36mDeploying Application...\033[0m\n"
	@sam deploy --template-file .aws-sam/build/packaged.yaml --stack-name cf-stack-$(PROJECT)-$(STAGE) --capabilities CAPABILITY_IAM \
		--parameter-overrides \
		Stage=$(STAGE) \
		ProjectName=$(PROJECT) \
		PythonVersion=$(PYTHON_VERSION)
	@printf "> \033[36mCompleted\033[0m\n"

inventory: ## deploy s3 inventory application
	@printf "> \033[36mDeploying Inventory...\033[0m\n"
	@sam package --template-file inventory.yaml --s3-bucket $(CODE_BUCKET) --output-template-file .aws-sam/build/inventory-packaged.yaml >/dev/null	
	@sam deploy --template-file .aws-sam/build/inventory-packaged.yaml --stack-name cf-stack-$(PROJECT)-inventory-$(STAGE) --capabilities CAPABILITY_IAM \
		--parameter-overrides \
		Stage=$(STAGE) \
		ProjectName=$(PROJECT) \
		StackName=cf-stack-$(PROJECT)-$(STAGE) \
		DataBaseName=$(DB_NAME) \
		PythonVersion=$(PYTHON_VERSION)\
		DataBucket=$$(aws cloudformation list-exports --query 'Exports[?Name==`cf-stack-$(PROJECT)-$(STAGE)-exports-data-bucket`].Value' --output text) 
	@printf "> \033[36mCompleted\033[0m\n"

redeploy: build package deploy ## build, package and deploy
	@printf " \033[33mAll services built, packaged and deployed!\033[0m\n"

all: runtime build package deploy ## build all (including runtime)
	@printf " \033[33mCreated runtime, built all services, packaged and deployed!\033[0m\n"
