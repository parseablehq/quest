FROM golang:1.21.1-bookworm

WORKDIR /tests

# Install all system dependencies
RUN apt update && apt install -y wget jq

# install k6
RUN gpg -k
RUN gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
RUN echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | tee /etc/apt/sources.list.d/k6.list
RUN apt update
RUN apt install k6

# Install flog for fake logs
RUN go install github.com/mingrammer/flog

# Copy source files and build test binary
COPY . .
RUN go test -c

# Make sure the shell script is executable
RUN chmod +x ./main.sh

ENTRYPOINT ["./main.sh"]
