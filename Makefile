# Makefile uses sh by default, but Github Actions (ubuntu-latest) requires dash/bash to work.
SHELL := /bin/bash

# VERSION defines the project version for the bundle.
# Update this value when you upgrade the version of your project.
# To re-generate a bundle for another specific version without changing the standard setup, you can:
# - use the VERSION as arg of the bundle target (e.g make bundle VERSION=0.0.2)
# - use environment variables to overwrite this value (e.g export VERSION=0.0.2)
VERSION ?= 0.0.1

# CHANNELS define the bundle channels used in the bundle.
# Add a new line here if you would like to change its default config. (E.g CHANNELS = "preview,fast,stable")
# To re-generate a bundle for other specific channels without changing the standard setup, you can:
# - use the CHANNELS as arg of the bundle target (e.g make bundle CHANNELS=preview,fast,stable)
# - use environment variables to overwrite this value (e.g export CHANNELS="preview,fast,stable")
ifneq ($(origin CHANNELS), undefined)
BUNDLE_CHANNELS := --channels=$(CHANNELS)
endif

# DEFAULT_CHANNEL defines the default channel used in the bundle.
# Add a new line here if you would like to change its default config. (E.g DEFAULT_CHANNEL = "stable")
# To re-generate a bundle for any other default channel without changing the default setup, you can:
# - use the DEFAULT_CHANNEL as arg of the bundle target (e.g make bundle DEFAULT_CHANNEL=stable)
# - use environment variables to overwrite this value (e.g export DEFAULT_CHANNEL="stable")
ifneq ($(origin DEFAULT_CHANNEL), undefined)
BUNDLE_DEFAULT_CHANNEL := --default-channel=$(DEFAULT_CHANNEL)
endif
BUNDLE_METADATA_OPTS ?= $(BUNDLE_CHANNELS) $(BUNDLE_DEFAULT_CHANNEL)

# IMAGE_TAG_BASE defines the docker.io namespace and part of the image name for remote images.
# This variable is used to construct full image tags for bundle and catalog images.
#
# For example, running 'make bundle-build bundle-push catalog-build catalog-push' will build and push both
# k8ssandra.io/k8ssandra-operator-bundle:$VERSION and k8ssandra.io/k8ssandra-operator-catalog:$VERSION.
IMAGE_TAG_BASE ?= k8ssandra/k8ssandra-operator

# BUNDLE_IMG defines the image:tag used for the bundle.
# You can use it as an arg. (E.g make bundle-build BUNDLE_IMG=<some-registry>/<project-name-bundle>:<tag>)
BUNDLE_IMG ?= $(IMAGE_TAG_BASE)-bundle:v$(VERSION)

# Image URL to use all building/pushing image targets
IMG ?= $(IMAGE_TAG_BASE):latest
# Create Kubernetes objects with embeddedObjectMeta
CRD_OPTIONS ?= "crd"

# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
# operator-sdk 1.11.9 bumps the k8s version to 1.21 but we have to temporarily downgrade due to
# https://github.com/kubernetes-sigs/controller-runtime/issues/1571
#ENVTEST_K8S_VERSION = 1.21
ENVTEST_K8S_VERSION = 1.23

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

KIND_CLUSTER ?= k8ssandra-0
GO_FLAGS=

# Setting SHELL to bash allows bash commands to be executed by recipes.
# This is a requirement for 'setup-envtest.sh' in the test target.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# Arguments to pass to e2e tests (does not affect other test types).
TEST_ARGS=

NS ?= k8ssandra-operator

# DEPLOYMENT specifies a particular kustomization to use for configuring the operator
# in a particular way, cluster-scoped for example. See config/deployments/README.md for
# more info.
DEPLOYMENT =

# Indicates the number of kind clusters that are being used. Note that the clusters should
# be created with scripts/setup-kind-multicluster.sh.
NUM_CLUSTERS = 2

# Indicates the number of worker nodes per cluster created with scripts/setup-kind-multicluster.sh.
# It can either be a single number or a comma-separated list of numbers, one per cluster.
NUM_WORKER_NODES = 4

ifeq ($(DEPLOYMENT), )
	DEPLOY_TARGET =
else
	DEPLOY_TARGET = /$(DEPLOYMENT)
endif

# The location of the kubeconfig file generated by setup-kind-multicluster.sh;
# this file is then read by create-clientconfig.sh.
KIND_KUBECONFIG ?= ./build/kind-kubeconfig

all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) $(CRD_OPTIONS) rbac:roleName=k8ssandra-operator webhook paths="./..." output:crd:artifacts:config=config/crd/bases

generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

fmt: ## Run go fmt against code.
	go fmt ./...

vet: ## Run go vet against code.
	go vet ./...

ENVTEST_ASSETS_DIR=$(shell pwd)/testbin
test: manifests generate fmt vet envtest ## Run tests.
ifdef TEST
	@echo Running test $(TEST)
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" go test $(GO_FLAGS) ./apis/... ./pkg/... ./test/yq/... ./controllers/... -run="$(TEST)" -covermode=atomic -coverprofile coverage.out
else
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" go test $(GO_FLAGS) ./apis/... ./pkg/... ./test/yq/... ./controllers/...  -covermode=atomic -coverprofile coverage.out
endif

E2E_TEST_TIMEOUT ?= 3600s

PHONY: e2e-test
e2e-test: ## Run e2e tests. Set E2E_TEST to run a specific test. Set TEST_ARGS to pass args to the test. You need to prepare the cluster(s) first by invoking single-prepare or multi-prepare.
ifdef E2E_TEST
	@echo Running e2e test $(E2E_TEST)
	go test -v -timeout $(E2E_TEST_TIMEOUT) ./test/e2e/... -run="$(E2E_TEST)" -args $(TEST_ARGS)
else
	@echo Running e2e tests
	go test -v -timeout $(E2E_TEST_TIMEOUT) ./test/e2e/... -args $(TEST_ARGS)
endif

##@ Build

build: generate fmt vet ## Build manager binary.
	go build -o bin/manager main.go

run: manifests generate fmt vet ## Run a controller from your host.
	go run ./main.go

docker-build:
	docker buildx build --load -t ${IMG} .

docker-push: ## Push docker image with the manager.
	docker push ${IMG}

kind-single-e2e-test: single-create single-prepare e2e-test

kind-multi-e2e-test: multi-create multi-prepare e2e-test

single-create: cleanup create-kind-cluster cert-manager traefik-kind

single-prepare: build manifests docker-build kind-load-image

single-up: single-create single-prepare kustomize
	kubectl config use-context kind-k8ssandra-0
	$(KUSTOMIZE) build config/deployments/control-plane$(DEPLOY_TARGET) | kubectl apply --server-side --force-conflicts -f -
	make NUM_CLUSTERS=1 create-clientconfig
	kubectl -n $(NS) delete pod -l control-plane=k8ssandra-operator
	kubectl -n $(NS) rollout status deployment k8ssandra-operator

single-reload: single-prepare kustomize
	kubectl config use-context kind-k8ssandra-0
	$(KUSTOMIZE) build config/deployments/control-plane$(DEPLOY_TARGET) | kubectl apply --server-side --force-conflicts -f -
	kubectl delete pod -l control-plane=k8ssandra-operator -n k8ssandra-operator
	kubectl rollout status deployment k8ssandra-operator -n k8ssandra-operator
ifeq ($(DEPLOYMENT), cass-operator-dev)
	kubectl -n $(NS) delete pod -l name=cass-operator
	kubectl -n $(NS) rollout status deployment cass-operator-controller-manager
endif

multi-create: cleanup create-kind-multicluster cert-manager-multi traefik-kind-multi

multi-prepare: build manifests docker-build kind-load-image-multi

multi-up: multi-create multi-prepare kustomize
## install the control plane
	kubectl config use-context kind-k8ssandra-0
	$(KUSTOMIZE) build config/deployments/control-plane$(DEPLOY_TARGET) | kubectl apply --server-side --force-conflicts -f -
##install the data plane
	for ((i = 1; i < $(NUM_CLUSTERS); ++i)); do \
		kubectl config use-context kind-k8ssandra-$$i; \
        $(KUSTOMIZE) build config/deployments/data-plane$(DEPLOY_TARGET) | kubectl apply --server-side --force-conflicts -f -; \
	done
## Create a client config
	make create-clientconfig
## Restart the control plane
	kubectl config use-context kind-k8ssandra-0
	kubectl -n $(NS) delete pod -l control-plane=k8ssandra-operator
	kubectl -n $(NS) rollout status deployment k8ssandra-operator
	kubectl -n $(NS) delete pod -l name=cass-operator
	kubectl -n $(NS) rollout status deployment cass-operator-controller-manager

multi-reload: multi-prepare kustomize
# Reload the operator on the control-plane
	kubectl config use-context kind-k8ssandra-0
	$(KUSTOMIZE) build config/deployments/control-plane$(DEPLOY_TARGET) | kubectl apply --server-side --force-conflicts -f -
	kubectl -n $(NS) delete pod -l control-plane=k8ssandra-operator
	kubectl -n $(NS) rollout status deployment k8ssandra-operator
	kubectl -n $(NS) delete pod -l name=cass-operator
	kubectl -n $(NS) rollout status deployment cass-operator-controller-manager
# Reload the operator on the data-plane
	for ((i = 1; i < $(NUM_CLUSTERS); ++i)); do \
    	kubectl config use-context kind-k8ssandra-$$i; \
    	$(KUSTOMIZE) build config/deployments/data-plane$(DEPLOY_TARGET) | kubectl apply --server-side --force-conflicts -f -; \
        kubectl -n $(NS) delete pod -l control-plane=k8ssandra-operator; \
        kubectl -n $(NS) rollout status deployment k8ssandra-operator; \
		kubectl -n $(NS) delete pod -l name=cass-operator; \
		kubectl -n $(NS) rollout status deployment cass-operator-controller-manager; \
	done

single-deploy:
	kubectl config use-context kind-k8ssandra-0
	kubectl -n $(NS) apply -f test/testdata/samples/k8ssandra-single-kind.yaml

multi-deploy:
	kubectl config use-context kind-k8ssandra-0
	kubectl -n $(NS) apply -f test/testdata/samples/k8ssandra-multi-kind.yaml

cleanup:
	for ((i = 0; i < $(NUM_CLUSTERS); ++i)); do \
		kind delete cluster --name k8ssandra-$$i; \
	done

create-kind-cluster:
	scripts/setup-kind-multicluster.sh --clusters 1 --kind-worker-nodes $(NUM_WORKER_NODES) --output-file $(KIND_KUBECONFIG)

create-kind-multicluster:
	scripts/setup-kind-multicluster.sh --clusters $(NUM_CLUSTERS) --kind-worker-nodes $(NUM_WORKER_NODES) --output-file $(KIND_KUBECONFIG)

kind-load-image:
	kind load docker-image --name $(KIND_CLUSTER) ${IMG}

kind-load-image-multi:
	for ((i = 0; i < $(NUM_CLUSTERS); ++i)); do \
		kind load docker-image --name k8ssandra-$$i ${IMG}; \
	done

##@ Deployment

install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | kubectl apply --server-side --force-conflicts -f -

uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | kubectl delete -f -

deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/deployments/control-plane$(DEPLOY_TARGET) | kubectl apply --server-side --force-conflicts -f -

undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/deployments/control-plane$(DEPLOY_TARGET) | kubectl delete -f -

cert-manager: ## Install cert-manager to the cluster
	kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.7.1/cert-manager.yaml
# Wait for cert-manager rollout to be fully done	
	kubectl rollout status deployment cert-manager -n cert-manager
	kubectl rollout status deployment cert-manager-cainjector -n cert-manager
	kubectl rollout status deployment cert-manager-webhook -n cert-manager

cert-manager-multi: ## Install cert-manager to the clusters
	for ((i = 0; i < $(NUM_CLUSTERS); ++i)); do \
		kubectl config use-context kind-k8ssandra-$$i; \
		make cert-manager;  \
	done

# Install Traefik in the current Kind cluster using Helm and a values file that is suitable for
# running e2e tests locally with a cluster created with setup-kind-multicluster.sh. Helm must be
# pre-installed on the system.
traefik-kind:
	helm repo add traefik https://helm.traefik.io/traefik
	helm repo update
	helm install traefik traefik/traefik --version v10.3.2 -f ./test/testdata/ingress/traefik.values.kind.yaml

# Install Traefik in all local Kind clusters.
traefik-kind-multi:
	for ((i = 0; i < $(NUM_CLUSTERS); ++i)); do \
		kubectl config use-context kind-k8ssandra-$$i; \
		make traefik-kind;  \
	done

traefik-uninstall:
	helm uninstall traefik

traefik-uninstall-kind-multi:
	for ((i = 0; i < $(NUM_CLUSTERS); ++i)); do \
		kubectl config use-context kind-k8ssandra-$$i; \
		make traefik-uninstall;  \
	done

# Installs Helm from an installation script. Note that on macOS it's better to install Helm with homebrew.
install-helm:
	mkdir -p ./bin ; \
	curl -fsSL -o ./bin/get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 ; \
	chmod 700 ./bin/get_helm.sh ; \
	./bin/get_helm.sh

create-clientconfig:
	for ((i = 0; i < $(NUM_CLUSTERS); ++i)); do \
		scripts/create-clientconfig.sh \
		  --namespace $(NS) \
		  --src-kubeconfig "$(KIND_KUBECONFIG)" \
		  --dest-kubeconfig "$(KIND_KUBECONFIG)" \
		  --src-context kind-k8ssandra-$$i \
		  --dest-context kind-k8ssandra-0; \
	done


CONTROLLER_GEN = $(shell pwd)/bin/controller-gen
controller-gen: ## Download controller-gen locally if necessary.
	$(call go-get-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen@v0.8.0)

KUSTOMIZE = $(shell pwd)/bin/kustomize
kustomize: ## Download kustomize locally if necessary.
	$(call go-get-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v4@v4.5.5)

ENVTEST = $(shell pwd)/bin/setup-envtest
envtest: ## Download envtest-setup locally if necessary.
	$(call go-get-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest@latest)

OS=$(shell go env GOOS)
ARCH=$(shell go env GOARCH)
.PHONY: operator-sdk
OPSDK = ./bin/operator-sdk
operator-sdk: ## Download operator-sdk locally if necessary
ifeq (,$(wildcard $(OPSDK)))
ifeq (,$(shell which operator-sdk 2>/dev/null))
	@{ \
	set -e ;\
	mkdir -p $(dir $(OPSDK)) ;\
	curl -sSLo $(OPSDK) https://github.com/operator-framework/operator-sdk/releases/download/v1.18.0/operator-sdk_${OS}_${ARCH} ;\
	chmod +x $(OPSDK) ;\
	}
else
OPSDK = $(shell which operator-sdk)
endif
endif

# go-get-tool will 'go get' any package $2 and install it to $1.
PROJECT_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
define go-get-tool
@[ -f $(1) ] || { \
set -e ;\
TMP_DIR=$$(mktemp -d) ;\
cd $$TMP_DIR ;\
go mod init tmp ;\
echo "Downloading $(2)" ;\
GOBIN=$(PROJECT_DIR)/bin go install $(2) ;\
rm -rf $$TMP_DIR ;\
}
endef

.PHONY: bundle
bundle: manifests kustomize ## Generate bundle manifests and metadata, then validate generated files.
	$(OPSDK) generate kustomize manifests -q
	cd config/manager && $(KUSTOMIZE) edit set image controller=$(IMG)
	$(KUSTOMIZE) build config/manifests | $(OPSDK) generate bundle -q --overwrite --version $(VERSION) $(BUNDLE_METADATA_OPTS)
	$(OPSDK) bundle validate ./bundle

.PHONY: bundle-build
bundle-build: ## Build the bundle image.
	docker build -f bundle.Dockerfile -t $(BUNDLE_IMG) .

.PHONY: bundle-push
bundle-push: ## Push the bundle image.
	$(MAKE) docker-push IMG=$(BUNDLE_IMG)

.PHONY: opm
OPM = ./bin/opm
opm: ## Download opm locally if necessary.
ifeq (,$(wildcard $(OPM)))
ifeq (,$(shell which opm 2>/dev/null))
	@{ \
	set -e ;\
	mkdir -p $(dir $(OPM)) ;\
	OS=$(shell go env GOOS) && ARCH=$(shell go env GOARCH) && \
	curl -sSLo $(OPM) https://github.com/operator-framework/operator-registry/releases/download/v1.19.5/$${OS}-$${ARCH}-opm ;\
	chmod +x $(OPM) ;\
	}
else
OPM = $(shell which opm)
endif
endif

# A comma-separated list of bundle images (e.g. make catalog-build BUNDLE_IMGS=example.com/operator-bundle:v0.1.0,example.com/operator-bundle:v0.2.0).
# These images MUST exist in a registry and be pull-able.
BUNDLE_IMGS ?= $(BUNDLE_IMG)

# The image tag given to the resulting catalog image (e.g. make catalog-build CATALOG_IMG=example.com/operator-catalog:v0.2.0).
CATALOG_IMG ?= $(IMAGE_TAG_BASE)-catalog:v$(VERSION)

# Set CATALOG_BASE_IMG to an existing catalog image tag to add $BUNDLE_IMGS to that image.
ifneq ($(origin CATALOG_BASE_IMG), undefined)
FROM_INDEX_OPT := --from-index $(CATALOG_BASE_IMG)
endif

# Build a catalog image by adding bundle images to an empty catalog using the operator package manager tool, 'opm'.
# This recipe invokes 'opm' in 'semver' bundle add mode. For more information on add modes, see:
# https://github.com/operator-framework/community-operators/blob/7f1438c/docs/packaging-operator.md#updating-your-existing-operator
.PHONY: catalog-build
catalog-build: opm ## Build a catalog image.
	$(OPM) index add --container-tool docker --mode semver --tag $(CATALOG_IMG) --bundles $(BUNDLE_IMGS) $(FROM_INDEX_OPT)

# Push the catalog image.
.PHONY: catalog-push
catalog-push: ## Push a catalog image.
	$(MAKE) docker-push IMG=$(CATALOG_IMG)

# E2E tests from kuttl
kuttl-test: install-kuttl docker-build
	./bin/kubectl-kuttl test --kind-context=k8ssandra-0 --start-kind=false --test test-servicemonitors
	./bin/kubectl-kuttl test --kind-context=k8ssandra-0 --start-kind=false --test test-cassandra-versions
	./bin/kubectl-kuttl test --kind-context=k8ssandra-0 --start-kind=false --test test-user-defined-ns

 # Install kuttl for e2e tests.
install-kuttl: 
	mkdir -p ./bin ; \
	cd ./bin ; \
	OS="$$(uname | tr '[:upper:]' '[:lower:]')" ; \
  	ARCH="$$(uname -m | sed -e 's/\(arm\)\(64\)\?.*/\1\2/' -e 's/aarch64$$/arm64/')" ; \
	curl -LO https://github.com/kudobuilder/kuttl/releases/download/v0.11.1/kuttl_0.11.1_$${OS}_$${ARCH}.tar.gz ; \
	tar -zxvf kuttl_0.11.1_$${OS}_$${ARCH}.tar.gz ; 

# Regenerate the mocks using mockery
mocks:
	mockery --dir=./pkg/cassandra --output=./pkg/mocks --name=ManagementApiFacade
	mockery --dir=./pkg/reaper --output=./pkg/mocks --name=Manager  --filename=reaper_manager.go --structname=ReaperManager


# The image base name used in GKE clusters.
GKE_IMAGE_TAG_BASE ?= us-docker.pkg.dev/community-ecosystem/$(IMAGE_TAG_BASE)

# Builds the project, builds the operator image, then pushes the operator image to GKE Container
# Registry.
gke-docker-push: build manifests
	$(MAKE) docker-build IMG=$(GKE_IMAGE_TAG_BASE):latest ; \
	$(MAKE) docker-push IMG=$(GKE_IMAGE_TAG_BASE):latest

# Runs e2e tests using two GKE contexts: k8ssandra-ci-us-east and k8ssandra-ci-us-north.
# The clusters must be up and running and your ~/.kube/config file must contain the corresponding
# contexts.
# For security reasons, instructions for setting up the clusters are not checked in version control.
# You must invoke this target with 2 env vars, GKE_US_EAST_IP and GKE_US_NORTH_IP containing 2
# valid and properly configured external IPs, e.g.:
# make gke-e2e-test GKE_US_EAST_IP=w.x.y.z GKE_US_NORTH_IP=w.x.y.z
gke-e2e-test:
	$(MAKE) e2e-test TEST_ARGS=" \
     -kubeconfigFile=$(HOME)/.kube/config \
     -controlPlane=k8ssandra-ci-us-east \
     -dataPlanes=k8ssandra-ci-us-east,k8ssandra-ci-us-north \
     -externalIPs=$(GKE_US_EAST_IP),$(GKE_US_NORTH_IP) \
     -zoneMappings='{ \
       \"region1-zone1\" : \"us-east1-b\", \
       \"region1-zone2\" : \"us-east1-c\", \
       \"region1-zone3\" : \"us-east1-d\", \
       \"region2-zone1\" : \"northamerica-northeast1-a\", \
       \"region2-zone2\" : \"northamerica-northeast1-b\", \
       \"region2-zone3\" : \"northamerica-northeast1-c\" \
     }' \
     -storage=standard-rwo \
     -hostNetwork=false \
     -imageName=$(GKE_IMAGE_TAG_BASE) \
     -imageTag=latest"

# The protobuf compiler is required to run this target: https://grpc.io/docs/protoc-installation/
PHONY: protobuf-code-gen
protobuf-code-gen:
	@protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pkg/medusa/medusa.proto
