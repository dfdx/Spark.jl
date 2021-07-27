#!/bin/bash

## This is a bootstrap action for installing Julia and Spark.jl on an Amazon EMR cluster.
## It's been tested with Julia 1.6.2 and EMR 5.33 and performs the following actions:
## 1. Installs Julia 1.6.2 and Maven 3.8.1
## 2. Configures the "hadoop" user's startup.jl to load Spark/Hadoop dependencies
## 3. Creates a shared package directory in which to install Spark.jl
## 4. Installs v0.5.1 of Spark.jl for the necessary Spark/Scala versions
## 
## You can run this script manually on every node or upload it to S3 and run it as a bootstrap action.
## When creating the EMR cluster, set the "spark-default" configuration with the following JSON.
## Reference: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html
#
# [
#   {
#     "Classification": "spark-defaults",
#     "Properties": {
#       "spark.executorEnv.JULIA_HOME": "/usr/local/julia-1.6.2/bin",
#       "spark.executorEnv.JULIA_DEPOT_PATH": "/usr/local/share/julia/v1.6.2",
#       "spark.executorEnv.JULIA_VERSION": "v1.6.2"
#     }
#   }
# ]

export JULIA_VERSION="1.6.2"
export JULIA_DL_URL="https://julialang-s3.julialang.org/bin/linux/x64/1.6/julia-1.6.2-linux-x86_64.tar.gz"

# install julia
curl -sL ${JULIA_DL_URL} | sudo tar -xz -C /usr/local/
JULIA_DIR=/usr/local/julia-${JULIA_VERSION}

# install maven
curl -s https://mirrors.sonic.net/apache/maven/maven-3/3.8.1/binaries/apache-maven-3.8.1-bin.tar.gz | sudo tar -xz -C /usr/local/
MAVEN_DIR=/usr/local/apache-maven-3.8.1

# Update the `hadoop` user's current and future path with Maven and Julia.
# This allows us to download/install Spark.jl
export PATH=${MAVEN_DIR}/bin:${JULIA_DIR}/bin:${PATH}
echo "export PATH=${MAVEN_DIR}/bin:${JULIA_DIR}/bin:${PATH}" >> /home/hadoop/.bashrc

# Create a shared package dir for the installation
sudo mkdir -p /usr/local/share/julia/v${JULIA_VERSION} && \
    sudo chown -R hadoop.hadoop /usr/local/share/julia/ && \
    sudo chmod -R go+r /usr/local/share/julia/

# Create a config file that adds Spark environment variables
# and adds the new package dir to the DEPOT_PATH.
# This ensures that Spark.jl gets installed to a shared location.
export TARGET_USER=hadoop
export JULIA_CFG_DIR="/home/${TARGET_USER}/.julia/config"
mkdir -p ${JULIA_CFG_DIR} && \
    touch ${JULIA_CFG_DIR}/startup.jl && \
    chown -R hadoop.hadoop /home/hadoop/.julia

echo 'ENV["SPARK_HOME"] = "/usr/lib/spark/"' >> "${JULIA_CFG_DIR}/startup.jl"
echo 'ENV["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"' >> "${JULIA_CFG_DIR}/startup.jl"
echo 'push!(DEPOT_PATH, "/usr/local/share/julia/v'${JULIA_VERSION}'")' >> "${JULIA_CFG_DIR}/startup.jl"

# Install Spark.jl - we need to explicity define Spark/Scala versions here
BUILD_SCALA_VERSION=2.11.12 \
BUILD_SPARK_VERSION=2.4.7 \
JULIA_COPY_STACKS=yes \
JULIA_DEPOT_PATH=/usr/local/share/julia/v${JULIA_VERSION} \
julia -e 'using Pkg;Pkg.add(Pkg.PackageSpec(;name="Spark", version="0.5.1"));using Spark;'
