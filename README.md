## Quest

This repository contains integration tests and load generation tests for Parseable server. Tests are written in shell script and bundled in a container.

### Build test container locally

```
docker build . -t parseable/quest
```

### Running tests

Use the below format to run tests against a Parseable server. The first argument is the test name, the second argument is the server URL, the third argument is the username and the fourth argument is the password.

```
docker run parseable/quest smoke http://demo.parseable.io parseable parseable
```
