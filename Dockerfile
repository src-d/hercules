FROM ubuntu:16.04

ENV GOPATH /root

RUN apt-get update && \
    apt-get install -y --no-install-suggests --no-install-recommends software-properties-common && \
    add-apt-repository -y ppa:gophers/archive && \
    apt-get update && \
    apt-get install -y --no-install-suggests --no-install-recommends locales golang-1.9-go python3 python3-dev libyaml-dev libyaml-0-2 libxml2-dev libxml2 curl git make unzip g++-5 && \
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-5 90 && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-5 90 && \
    curl -SLo protoc.zip https://github.com/google/protobuf/releases/download/v3.5.0/protoc-3.5.0-linux-x86_64.zip && \
    unzip -d /usr/local protoc.zip && rm protoc.zip && \
    locale-gen en_US.UTF-8 && \
    export PATH=/usr/lib/go-1.9/bin:$PATH && \
    go get -v -d gopkg.in/src-d/hercules.v3/... && \
    cd /root/src/gopkg.in/src-d/hercules.v3 && \
    export CGO_CXXFLAGS=-std=c++14 && \
    make && \
    rm /usr/local/bin/protoc && rm /usr/local/readme.txt && rm -rf /usr/local/include/google && \
    cp /root/bin/hercules /usr/local/bin && \
    cp -r /root/src/gopkg.in/src-d/hercules.v3/*.py /root/src/gopkg.in/src-d/hercules.v3/pb /usr/local/bin && \
    sed -i 's/parser.add_argument("--backend",/parser.add_argument("--backend", default="Agg",/' /usr/local/bin/labours.py && \
    echo '#!/bin/bash\n\
\n\
echo\n\
echo "	$@"\n\
echo\n\' > /browser && \
    chmod +x /browser && \
    curl https://bootstrap.pypa.io/get-pip.py | python3 && \
    pip3 install --no-cache-dir -r /root/src/gopkg.in/src-d/hercules.v3/requirements.txt https://github.com/mind/wheels/releases/download/tf1.3-cpu/tensorflow-1.3.0-cp35-cp35m-linux_x86_64.whl && \
    rm -rf /root/* && \
    apt-get remove -y software-properties-common golang-1.9-go python3-dev libyaml-dev libxml2-dev curl git make unzip g++-5 && \
    apt-get remove -y *-doc *-man && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

EXPOSE 8000
ENV BROWSER /browser
ENV LC_ALL en_US.UTF-8
ENV COUPLES_SERVER_TIME 7200
