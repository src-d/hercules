FROM golang:1.13 AS builder
ENV PROTOBUF_VERSION 3.5.1
COPY . /root/src
RUN apt-get update && \
    apt-get install -y unzip make && \
    curl -SLo protoc.zip https://github.com/google/protobuf/releases/download/v$PROTOBUF_VERSION/protoc-$PROTOBUF_VERSION-linux-x86_64.zip && \
    unzip -d /usr/local protoc.zip && \
    rm protoc.zip && \
    cd /root/src && \
    curl -L "https://storage.googleapis.com/tensorflow/libtensorflow/libtensorflow-cpu-$(go env GOOS)-x86_64-1.7.0.tar.gz" | tar -C /usr/local -xz && \
    make

FROM ubuntu:18.04
COPY --from=builder /root/src/hercules /usr/local/bin
COPY python /root/src
ENV LC_ALL en_US.UTF-8
RUN apt-get update && \
    apt-get upgrade -y  && \
    apt-get install -y --no-install-suggests --no-install-recommends locales ca-certificates python3 python3-dev python3-distutils libyaml-dev libyaml-0-2 libxml2-dev libxml2 curl git g++ && \
    locale-gen en_US.UTF-8 && \
    echo '#!/bin/bash\n\
\n\
echo\n\
echo "	$@"\n\
echo\n\' > /browser && \
    chmod +x /browser && \
    curl https://bootstrap.pypa.io/get-pip.py | python3 - pip==18.1 && \
    pip3 install --no-cache-dir --no-build-isolation cython && \
    sed -i 's/DEFAULT_MATPLOTLIB_BACKEND = None/DEFAULT_MATPLOTLIB_BACKEND = "Agg"/' /root/src/labours/cli.py && \
    pip3 install --no-cache-dir /root/src && \
    pip3 install --no-cache-dir "tensorflow<2.0" && \
    rm -rf /root/src && \
    apt-get remove -y python3-dev libyaml-dev libxml2-dev curl git g++ && \
    apt-get autoremove -y && \
    rm -rf /usr/share/doc /usr/share/man && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

EXPOSE 8000
ENV BROWSER /browser
ENV COUPLES_SERVER_TIME 7200
