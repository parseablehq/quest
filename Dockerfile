FROM golang:1.21.1-bookworm

WORKDIR /tests

COPY . .

RUN go test -c \
    && apt install wget \
    && wget https://github.com/grafana/k6/releases/download/v0.46.0/k6-v0.46.0-linux-amd64.deb \
    && apt install -f ./k6-v0.46.0-linux-amd64.deb \
    && apt update \
    && apt install -y jq \
    && wget https://github.com/mingrammer/flog/releases/download/v0.4.3/flog_0.4.3_linux_amd64.tar.gz \
    && tar -xvf flog_0.4.3_linux_amd64.tar.gz \
    && cp flog /usr/local/bin 

ENTRYPOINT ["./main.sh"]
