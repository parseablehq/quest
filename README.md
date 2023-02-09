## Quest

This repository contains integration tests and load generation tests for Parseable server. Tests are written in shell script and bundled in a container.

### Build test container locally

```
docker build . -t parseable/quest:v0.1
```

### Running tests

Use the below format to run tests against a Parseable server. The first argument is the test name, the second argument is the server URL, the third argument is the username and the fourth argument is the password.

```
docker run parseable/quest:v0.1 smoke http://demo.parseable.io parseable parseable
```

If you want to run tests against a local Parseable server, you can use the following command:

```
docker run parseable/quest:v0.1 load http://host.docker.internal:8000 admin admin --network="host"
```
