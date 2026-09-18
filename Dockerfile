FROM ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.158.0 AS telemetrygen

FROM golang:1.21.1-bookworm

WORKDIR /tests

COPY . .

COPY --from=telemetrygen /telemetrygen /usr/local/bin/telemetrygen

RUN go test ./tests/integration/clients/... \
    && go test -c -o quest.test ./tests/integration \
    && apt -y update \
    && apt -y install wget \
    && wget https://github.com/grafana/k6/releases/download/v0.46.0/k6-v0.46.0-linux-amd64.deb \
    && apt install -y -f ./k6-v0.46.0-linux-amd64.deb \
    && apt install -y jq \
    && wget https://github.com/mingrammer/flog/releases/download/v0.4.3/flog_0.4.3_linux_amd64.tar.gz \
    && tar -xvf flog_0.4.3_linux_amd64.tar.gz \
    && cp flog /usr/local/bin \
    && wget https://github.com/parseablehq/pb/releases/download/v1.0.1/pb_1.0.1_linux_amd64.tar.gz \
    && tar -xzf pb_1.0.1_linux_amd64.tar.gz pb \
    && install -m 0755 pb /usr/local/bin/pb

ENTRYPOINT ["./main.sh"]
