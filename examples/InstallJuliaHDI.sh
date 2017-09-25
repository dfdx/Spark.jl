#!/usr/bin/env bash

# An example shell script that can be used on Azure HDInsight to install Julia to HDI Spark cluster
# This script, or a derivative should be set as a script action when deploying an HDInsight cluster

# install julia v0.6
curl -s https://julialang-s3.julialang.org/bin/linux/x64/0.6/julia-0.6.0-linux-x86_64.tar.gz | sudo tar -xz -C /usr/local/
JULIA_HOME=/usr/local/julia-903644385b/bin

# install maven
curl -s http://mirror.olnevhost.net/pub/apache/maven/binaries/apache-maven-3.2.2-bin.tar.gz | sudo tar -xz -C /usr/local/
export M2_HOME=/usr/local/apache-maven-3.2.2
export PATH=$M2_HOME/bin:$PATH

# Create Directories
export JULIA_PKGDIR="/home/hadoop/.julia/"
mkdir -p ${JULIA_PKGDIR}

# Set Environment variables for current session
export PATH=${PATH}:${MVN_HOME}/bin:${JULIA_HOME}/bin
export HOME="/root"
echo "Installing Julia Packages in Julia Folder ${JULIA_PKGDIR}"
#Install Spark.jl
$JULIA_HOME/julia -e 'Pkg.add("Spark");Pkg.checkout("Spark");Pkg.build("Spark"); using Spark;'
declare -a users=("spark" "yarn" "hadoop" "sshuser") #TODO: change accordingly
SPARK_HOME=/usr/hdp/current/spark2-client
for cusr in "${users[@]}"; do
   echo " Adding vars for ser ${cusr}"
   echo "" >> /home/${cusr}/.bashrc
   echo "export MVN_HOME=/usr/local/apache-maven-3.2.2" >> /home/${cusr}/.bashrc
   echo "export PATH=${PATH}:${MVN_HOME}/bin:${JULIA_HOME}" >> /home/${cusr}/.bashrc   
   echo "export YARN_CONF_DIR=/etc/hadoop/conf" >> /home/${cusr}/.bashrc
   echo "export JULIA_HOME=${JULIA_HOME}" >> /home/${cusr}/.bashrc
   echo "export JULIA_PKGDIR=${JULIA_PKGDIR}" >> /home/${cusr}/.bashrc   
   echo "source ${SPARK_HOME}/bin/load-spark-env.sh" >> /home/${cusr}/.bashrc 
   echo "spark.executorEnv.JULIA_HOME ${JULIA_HOME}" >> ${SPARK_HOME}/conf/spark-defaults.conf
   echo "spark.executorEnv.JULIA_PKGDIR ${JULIA_PKGDIR}" >> ${SPARK_HOME}/conf/spark-defaults.conf
   echo "spark.executorEnv.JULIA_VERSION v0.6" >> ${SPARK_HOME}/conf/spark-defaults.conf
   # Set Package folder permissions   
   setfacl -R -m u:${cusr}:rwx ${JULIA_PKGDIR};  
done