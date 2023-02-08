FROM registry.access.redhat.com/ubi8/ubi:8.1

WORKDIR /tests

COPY main.sh .
COPY testcases/smoke_test.sh ./testcases/

RUN yum -y install wget
RUN yum -y install jq

RUN wget https://github.com/mingrammer/flog/releases/download/v0.4.3/flog_0.4.3_linux_amd64.tar.gz \
  && tar -xvf flog_0.4.3_linux_amd64.tar.gz \
  && cp flog /usr/local/bin

ENTRYPOINT ["./main.sh"]
