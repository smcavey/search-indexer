# Copyright Contributors to the Open Cluster Management project

FROM registry.ci.openshift.org/stolostron/builder:go1.23-linux AS builder

WORKDIR /go/src/github.com/stolostron/search-indexer
COPY . .
RUN CGO_ENABLED=1 go build -trimpath -o main main.go

FROM registry.access.redhat.com/ubi9/ubi-minimal:latest

COPY --from=builder /go/src/github.com/stolostron/search-indexer/main /bin/main

ENV VCS_REF="$VCS_REF" \
    USER_UID=1001

EXPOSE 3010
USER ${USER_UID}
ENTRYPOINT ["/bin/main"]
