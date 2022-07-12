FROM ubuntu:bionic

RUN apt-get update \
 && apt-get install -y \
 curl \
 gcc \
 libssl-dev \
 libzmq3-dev \
 pkg-config \
 unzip

RUN \
 if [ ! -z $HTTP_PROXY ] && [ -z $http_proxy ]; then \
  http_proxy=$HTTP_PROXY; \
 fi; \
 if [ ! -z $HTTPS_PROXY ] && [ -z $https_proxy ]; then \
  https_proxy=$HTTPS_PROXY; \
 fi; \
 if [ ! -z $http_proxy ]; then \
  http_proxy_host=$(printf $http_proxy | sed 's|http.*://\(.*\):\(.*\)$|\1|');\
  http_proxy_port=$(printf $http_proxy | sed 's|http.*://\(.*\):\(.*\)$|\2|');\
  mkdir -p $HOME/.cargo \
  && echo "[http]" >> $HOME/.cargo/config \
  && echo 'proxy = "'$http_proxy_host:$http_proxy_port'"' >> $HOME/.cargo/config \
  && cat $HOME/.cargo/config; \
 fi;

# For Building Protobufs
RUN curl -OLsS https://github.com/google/protobuf/releases/download/v3.5.1/protoc-3.5.1-linux-x86_64.zip \
 && unzip protoc-3.5.1-linux-x86_64.zip -d protoc3 \
 && rm protoc-3.5.1-linux-x86_64.zip

RUN curl https://sh.rustup.rs -sSf > /usr/bin/rustup-init \
 && chmod +x /usr/bin/rustup-init \
 && rustup-init -y

ENV PATH=$PATH:/home/rust-sawtooth/target/release/:/protoc3/bin:/root/.cargo/bin

# WORKDIR /home/sami/projects/tweet_audit/rust-sawtooth
COPY ./src/ /home/rust-sawtooth/src/
COPY ./Cargo.toml /home/rust-sawtooth/
RUN echo $(ls /home/rust-sawtooth)
WORKDIR /home/rust-sawtooth

RUN cargo build -r
EXPOSE 4004/tcp

CMD ["twit-tp-rust"]

