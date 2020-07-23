FROM python:3.7.3

RUN PROTOC_ZIP=protoc-3.7.1-linux-x86_64.zip && curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v3.7.1/$PROTOC_ZIP
RUN PROTOC_ZIP=protoc-3.7.1-linux-x86_64.zip && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc
RUN PROTOC_ZIP=protoc-3.7.1-linux-x86_64.zip && unzip -o $PROTOC_ZIP -d /usr/local 'include/*'
RUN PROTOC_ZIP=protoc-3.7.1-linux-x86_64.zip && rm -f $PROTOC_ZIP

RUN git clone https://github.com/jkim1014/tfx-async.git
WORKDIR /tfx-async
RUN pip install -e .