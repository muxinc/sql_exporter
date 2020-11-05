FROM quay.io/prometheus/golang-builder as builder

ADD .   /go/src/github.com/justwatchcom/sql_exporter
WORKDIR /go/src/github.com/justwatchcom/sql_exporter

RUN make

FROM        quay.io/prometheus/busybox:glibc
COPY --from=builder /go/src/github.com/justwatchcom/sql_exporter/sql_exporter  /bin/sql_exporter

EXPOSE      9237
ENTRYPOINT  [ "/bin/sql_exporter" ]
