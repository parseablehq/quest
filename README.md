## Quest

This repository contains integration tests and load generation tests for Parseable server. Tests are written in shell script and bundled in a container.

### Use pre-built container

Pre-built container is available on GitHub Container Registry. You can pull the container using the following command:

```
docker pull ghcr.io/parseablehq/quest:main
```

### Build test container locally

```
docker build . -t parseable/quest:v0.1
```

### Running tests

Use the below format to run tests against a Parseable server. The first argument is the test name, the second argument is the server URL, the third argument is the username and the fourth argument is the password.

```
docker run ghcr.io/parseablehq/quest:main smoke https://demo.parseable.io parseable parseable
```

If you want to run tests against a local Parseable server, you can use the following command:

```
docker run ghcr.io/parseablehq/quest:main load http://host.docker.internal:8000 admin admin --network="host"
```

#### Kubernetes

To run tests against a Parseable server running on Kubernetes, you can use the Job resource. Refer [sample job manifest](./kubernetes/job.yaml). Modify the `command` section to run the tests you want. You can run the job using the following command:

```
kubectl apply -f kubernetes/job.yaml
```
