FROM ubuntu:18.04

ENV PROTOBUF_VERSION 3.5.1
ENV GO111MODULE on

COPY . /root/src
RUN apt-get update && \
    apt-get install -y --no-install-suggests --no-install-recommends software-properties-common && \
    add-apt-repository -y ppa:gophers/archive && \
    apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-suggests --no-install-recommends locales golang-1.11-go python3 python3-dev python3-distutils libyaml-dev libyaml-0-2 libxml2-dev libxml2 curl git make unzip g++ && \
    curl -SLo protoc.zip https://github.com/google/protobuf/releases/download/v$PROTOBUF_VERSION/protoc-$PROTOBUF_VERSION-linux-x86_64.zip && \
    unzip -d /usr/local protoc.zip && rm protoc.zip && \
    locale-gen en_US.UTF-8 && \
    export PATH=/usr/lib/go-1.11/bin:/root/bin:$PATH && \
    cd /root/src && \
    curl -L "https://storage.googleapis.com/tensorflow/libtensorflow/libtensorflow-cpu-$(go env GOOS)-x86_64-1.7.0.tar.gz" | tar -C /usr/local -xz && \
    make && \
    rm /usr/local/bin/protoc /usr/local/readme.txt && rm -rf /usr/local/include/google && \
    mv hercules /usr/local/bin && \
    echo '#!/bin/bash\n\
\n\
echo\n\
echo "	$@"\n\
echo\n\' > /browser && \
    chmod +x /browser && \
    curl https://bootstrap.pypa.io/get-pip.py | python3 - pip==18.1 && \
    pip3 install --no-cache-dir --no-build-isolation cython && \
    sed -i 's/parser.add_argument("--backend",/parser.add_argument("--backend", default="Agg",/' /root/src/python/labours/labours.py && \
    pip3 install --no-cache-dir /root/src/python && \
    pip3 install --no-cache-dir "tensorflow<2.0" && \
    rm -rf /root/* && \
    apt-get remove -y software-properties-common golang-1.11-go python3-dev libyaml-dev libxml2-dev curl git make unzip g++ && \
    apt-get remove -qy *-doc *-man && \
    rm -rf /usr/share/doc /usr/share/man && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

EXPOSE 8000
ENV BROWSER /browser
ENV LC_ALL en_US.UTF-8
ENV COUPLES_SERVER_TIME 7200
