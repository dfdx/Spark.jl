# FROM apache/spark:3.5.3-scala2.12-java17-python3-r-ubuntu
FROM ubuntu:22.04


## Basic devcontainer setup

ENV user=spark

ENV JAVA_VERSION=8

ENV SCALA_VERSION=2.12
ENV BINARY_SCALA_VERSION=2.12.12

ENV JULIA_VERSION=1.10.5
ENV JULIA_VERSION_SHORT=1.10

ENV SPARK_VERSION=3.5.3

ENV KAFKA_VERSION=${SCALA_VERSION}-3.8.0
ENV KAFKA_VERSION_SHORT=3.8.0

ENV TERM=xterm-color

ENV DEBIAN_FRONTEND=noninteractive \
    TERM=linux

ENV LANGUAGE=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8 \
    LC_CTYPE=en_US.UTF-8 \
    LC_MESSAGES=en_US.UTF-8

# -------------------------- SUDO LAND ------------------------------

USER root

RUN apt update && apt install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        git \
        gpg \
        gpg-agent \
        less \
        libbz2-dev \
        libffi-dev \
        liblzma-dev \
        libncurses5-dev \
        libncursesw5-dev \
        libreadline-dev \
        libsqlite3-dev \
        libssl-dev \
        llvm \
        locales \
        tk-dev \
        tzdata \
        unzip \
        vim \
        wget \
        xz-utils \
        zlib1g-dev \
        zstd \
    && sed -i "s/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g" /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && apt clean


## System packages

RUN apt-get update
RUN apt-get install -y git openssh-server


## Add user & enable sudo

# RUN id -u ${user} &>/dev/null || useradd -ms /bin/bash ${user}
RUN id -u ${user} || useradd -ms /bin/bash ${user}
RUN usermod -aG sudo ${user}

RUN apt-get install -y sudo
RUN echo "${user} ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

RUN chsh -s /bin/bash ${user}

# ensure home directory exists
RUN mkdir -p /home/${user}
RUN chown -R ${user}:${user} /home/${user}

RUN touch /home/${user}/.bashrc
RUN chown ${user}:${user} /home/${user}/.bashrc


## Julia

WORKDIR /opt
RUN curl --output julia.tgz https://julialang-s3.julialang.org/bin/linux/aarch64/${JULIA_VERSION_SHORT}/julia-${JULIA_VERSION}-linux-aarch64.tar.gz
RUN tar -xzf julia.tgz
RUN echo "export PATH=\${PATH}:/opt/julia-${JULIA_VERSION}/bin" >> /home/${user}/.bashrc
RUN rm julia.tgz


## Java & Maven

RUN apt-get install -y openjdk-${JAVA_VERSION}-jdk
RUN update-alternatives --set java $(update-alternatives --list java | grep java-${JAVA_VERSION}) || true
RUN update-alternatives --set javac $(update-alternatives --list java | grep java-${JAVA_VERSION}) || true

RUN apt-get install -y maven


## Spark
WORKDIR /opt
RUN curl --output spark.tgz https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz
RUN tar -xzf spark.tgz
RUN mv spark-${SPARK_VERSION}-bin-hadoop3 spark



## Kafka
WORKDIR /opt
RUN curl --output kafka.tgz https://dlcdn.apache.org/kafka/${KAFKA_VERSION_SHORT}/kafka_${KAFKA_VERSION}.tgz
RUN tar -xzf kafka.tgz
RUN echo "export PATH=\${PATH}:/opt/kafka_${KAFKA_VERSION}/bin" >> /home/${user}/.bashrc
RUN rm kafka.tgz

RUN mkdir -p /opt/kafka_${KAFKA_VERSION}/logs
RUN chmod -R a+rwx /opt/kafka_${KAFKA_VERSION}/logs


# -------------------------- USER LAND ------------------------------

## Spark.jl

USER ${user}

ENV BUILD_SCALA_VERSION=${SCALA_VERSION}
ENV BUILD_BINARY_SCALA_VERSION=${BINARY_SCALA_VERSION}
ENV BUILD_SPARK_VERSION=${SPARK_VERSION}

# TODO: uncomment and test
# RUN /opt/julia-${JULIA_VERSION}/bin/julia -e 'import Pkg; Pkg.add("Spark")'


## Launch

CMD ["echo", "Hello Spark"]