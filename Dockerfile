FROM amazonlinux:2

ENV GOPATH /.go
ENV PATH $PATH:/usr/local/go/bin:$GOPATH/bin
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

RUN mkdir -p /.go/src/memcache
RUN yum install -y gcc tar wget vim memcached
RUN ARCH=$(uname -m) && \
    case "$ARCH" in \
        x86_64)  GOARCH=amd64 ;; \
        aarch64) GOARCH=arm64 ;; \
        *) echo "unsupported arch: $ARCH" && exit 1 ;; \
    esac && \
    wget "https://go.dev/dl/go1.25.1.linux-${GOARCH}.tar.gz" && \
    tar -xzf go1.25.1.linux-${GOARCH}.tar.gz -C /usr/local && \
    rm go1.25.1.linux-${GOARCH}.tar.gz
WORKDIR /.go/src/memcache
EXPOSE 11211
