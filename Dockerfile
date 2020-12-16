FROM amazonlinux:2

ENV GOPATH /.go
ENV PATH $PATH:/usr/local/go/bin:$GOPATH/bin
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

RUN mkdir -p /.go/src/memcache
RUN yum install -y gcc tar wget vim memcached
RUN wget 'https://redirector.gvt1.com/edgedl/go/go1.15.linux-amd64.tar.gz' && \
    tar -xzf go1.15.linux-amd64.tar.gz && \
    mv go /usr/local && \
    rm go1.15.linux-amd64.tar.gz
WORKDIR /.go/src/memcache
EXPOSE 11211
