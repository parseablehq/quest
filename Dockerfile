FROM golang:1.21.1-bookworm

WORKDIR /tests

COPY . .

RUN go test ./tests/integration/clients/... \
    && go test -c -o quest.test ./tests/integration \
    && apt install wget \
    && wget https://github.com/grafana/k6/releases/download/v0.46.0/k6-v0.46.0-linux-amd64.deb \
    && apt install -f ./k6-v0.46.0-linux-amd64.deb \
    && apt update \
    && apt install -y jq \
    && wget https://github.com/mingrammer/flog/releases/download/v0.4.3/flog_0.4.3_linux_amd64.tar.gz \
    && tar -xvf flog_0.4.3_linux_amd64.tar.gz \
    && cp flog /usr/local/bin \
    && pb_release_url=$(wget -qO- --server-response https://github.com/parseablehq/pb/releases/latest 2>&1 | awk '/^  Location: / { url=$2 } END { sub(/\r$/, "", url); print url }') \
    && pb_version=${pb_release_url##*/v} \
    && wget https://github.com/parseablehq/pb/releases/download/v${pb_version}/pb_${pb_version}_linux_amd64.tar.gz \
    && wget https://github.com/parseablehq/pb/releases/download/v${pb_version}/pb_${pb_version}_checksums.txt \
    && grep "pb_${pb_version}_linux_amd64.tar.gz" pb_${pb_version}_checksums.txt | sha256sum -c - \
    && tar -xzf pb_${pb_version}_linux_amd64.tar.gz pb \
    && install -m 0755 pb /usr/local/bin/pb \
    && pb --help > /dev/null

ENTRYPOINT ["./main.sh"]
