FROM registry.access.redhat.com/ubi8/ubi:8.1

WORKDIR /tests

COPY . .

RUN yum -y install wget \
    && yum -y install https://dl.k6.io/rpm/repo.rpm \
    && yum -y install k6 \
    && yum -y install jq \
    && wget https://github.com/mingrammer/flog/releases/download/v0.4.3/flog_0.4.3_linux_amd64.tar.gz \
    && tar -xvf flog_0.4.3_linux_amd64.tar.gz \
    && cp flog /usr/local/bin 

ENTRYPOINT ["./main.sh"]
