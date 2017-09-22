FROM ubuntu:16.04

ENV GOPATH /root

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository -y ppa:gophers/archive && \
    apt-get update && \
    apt-get install -y golang-1.9-go python3 python3-dev libyaml-dev libyaml-0-2 curl git && \
    /usr/lib/go-*/bin/go get -v gopkg.in/src-d/hercules.v2/cmd/hercules && \
    cp /root/bin/hercules /usr/local/bin && \
    cp -r /root/src/gopkg.in/src-d/hercules.v2/*.py /root/src/gopkg.in/src-d/hercules.v2/pb /usr/local/bin && \
    sed -i 's/parser.add_argument("--backend",/parser.add_argument("--backend", default="Agg",/' /usr/local/bin/labours.py && \
    curl https://bootstrap.pypa.io/get-pip.py | python3 && \
    pip3 install --no-cache-dir -r /root/src/gopkg.in/src-d/hercules.v2/requirements.txt && \
    rm -rf /root/* && \
    apt-get remove -y software-properties-common golang-1.9-go python3-dev libyaml-dev curl git && \
    apt-get remove *-doc && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean
