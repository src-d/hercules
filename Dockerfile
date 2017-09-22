FROM ubuntu:16.04

ENV GOPATH /root

RUN apt-get update && \
    apt-get install -y --no-install-suggests --no-install-recommends software-properties-common && \
    add-apt-repository -y ppa:gophers/archive && \
    apt-get update && \
    apt-get install -y --no-install-suggests --no-install-recommends locales golang-1.9-go python3 python3-dev libyaml-dev libyaml-0-2 curl git && \
    locale-gen en_US.UTF-8 && \
    /usr/lib/go-*/bin/go get -v gopkg.in/src-d/hercules.v2/cmd/hercules && \
    cp /root/bin/hercules /usr/local/bin && \
    cp -r /root/src/gopkg.in/src-d/hercules.v2/*.py /root/src/gopkg.in/src-d/hercules.v2/pb /usr/local/bin && \
    sed -i 's/parser.add_argument("--backend",/parser.add_argument("--backend", default="Agg",/' /usr/local/bin/labours.py && \
    echo '#!/bin/bash\n\
\n\
echo\n\
echo "	$@"\n\
echo\n\' > /browser && \
    chmod +x /browser && \
    curl https://bootstrap.pypa.io/get-pip.py | python3 && \
    pip3 install --no-cache-dir -r /root/src/gopkg.in/src-d/hercules.v2/requirements.txt https://github.com/mind/wheels/releases/download/tf1.3-cpu/tensorflow-1.3.0-cp35-cp35m-linux_x86_64.whl && \
    rm -rf /root/* && \
    apt-get remove -y software-properties-common golang-1.9-go python3-dev libyaml-dev curl git && \
    apt-get remove -y *-doc *-man && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

EXPOSE 8000
ENV BROWSER /browser
ENV LC_ALL en_US.UTF-8
ENV COUPLES_SERVER_TIME 7200
