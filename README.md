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

Use the below format to run tests against a Parseable server.

Positional arguments for the 'smoke' or 'load' mode:

```
1. Test name: `smoke` or `load`
2. Server URL
3. Username
4. Password 
8. MinIO URL (Shouldn't be prefixed with `http://`. e.g. `localhost:9000`. Note that `https` isn't supported yet. )
9. MinIO Access Key (User)
10. MinIO Secret Key (Password)
11. MinIO Bucket (name of the bucket Parseable is configured to ingest into)
```

Additional positional arguments for the 'load' mode
```
5. (Optional) Number of different json schemas to send to a stream
6. (Optional) Number of virtual users (Refer K6 [documentation on VUs](https://k6.io/docs/get-started/running-k6/#adding-more-vus))
7. (Optional) Duration of the test
```

Example usage:
```
docker run ghcr.io/parseablehq/quest:main smoke https://demo.parseable.io parseable parseable
```

If you want to run tests against a local Parseable server, you can use the following command:

```
docker run --network="host" ghcr.io/parseablehq/quest:main load http://host.docker.internal:8000 admin admin 20 10 5m
```

#### Kubernetes

To run tests against a Parseable server running on Kubernetes, you can use the Job resource. Refer [sample job manifest](./kubernetes/job.yaml). Modify the `command` section to run the tests you want. You can run the job using the following command:

```
kubectl apply -f kubernetes/job.yaml
```
