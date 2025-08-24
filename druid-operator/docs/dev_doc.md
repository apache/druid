## Dev Dependencies

- Golang 1.20+
- Kubebuilder v3
- It is recommended to install kind since the project's e2e tests are using it.

## Running Operator Locally
We're using Kubebuilder so we are working with its `Makefile` and extra custom commands:
```shell
# If needed, create a kubernetes cluster (requires kind)
make kind

# Install the CRDs
make install

# Run the operator locally
make run
```

## Watch a namespace
```shell
# Watch all namespaces
export WATCH_NAMESPACE=""

# Watch a single namespace
export WATCH_NAMESPACE="mynamespace"

# Watch all namespaces except: kube-system, default
export DENY_LIST="kube-system,default" 
```

## Building The Operator Docker Image
```shell
make docker-build

# In case you want to build it with a custom image:
make docker-build IMG=custom-name:custom-tag 
```

## Testing
Before submitting a PR, make sure the tests are running successfully.
```shell
# Run unit tests
make test 

# Run E2E tests (requires kind)
make e2e
```

## Documentation
If you changed the CRD API, please make sure the documentation is also updated:
```shell
make api-docs 
```

## Help
The `Makefile` should contain all commands with explanations. You can also run:
```shell
# For help
make help
```
