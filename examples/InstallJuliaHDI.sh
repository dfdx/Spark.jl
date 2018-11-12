#!/usr/bin/env bash

# An example shell script that can be used on Azure HDInsight to install Julia to HDI Spark cluster
# This script, or a derivative should be set as a script action when deploying an HDInsight cluster

# install julia v0.6
curl -s https://julialang-s3.julialang.org/bin/linux/x64/1.0/julia-1.0.2-linux-x86_64.tar.gz | sudo tar -xz -C /usr/local/
JULIA_HOME=/usr/local/julia-1.0.2/bin

#Patch Julia for #28815
cat <<EOT >> $JULIA_HOME/../etc/julia/startup.jl
function Base.touch(path::AbstractString)
    f = Base.Filesystem.open(path, Base.JL_O_WRONLY | Base.JL_O_CREAT, 0o0666)
    try
        if Sys.isunix()
            ret = ccall(:futimes, Cint, (Cint, Ptr{Cvoid}), fd(f), C_NULL)
            systemerror(:futimes, ret != 0, extrainfo=path)
        else
            t = time()
            futime(f,t,t)
        end
    finally
        close(f)
    end
    path
end
EOT

# install maven
curl -s http://mirror.olnevhost.net/pub/apache/maven/binaries/apache-maven-3.2.2-bin.tar.gz | sudo tar -xz -C /usr/local/
export M2_HOME=/usr/local/apache-maven-3.2.2
export PATH=$M2_HOME/bin:$PATH

# Create Directories
export JULIA_DEPOT_PATH="/home/hadoop/.julia/"
mkdir -p ${JULIA_DEPOT_PATH}

# Set Environment variables for current session
export PATH=${PATH}:${MVN_HOME}/bin:${JULIA_HOME}/bin
export HOME="/root"
echo "Installing Julia Packages in Julia Folder ${JULIA_DEPOT_PATH}"
#Install Spark.jl
$JULIA_HOME/julia -e 'using Pkg; Pkg.add("Spark");Pkg.build("Spark"); using Spark;'
declare -a users=("spark" "yarn" "hadoop" "sshuser") #TODO: change accordingly
SPARK_HOME=/usr/hdp/current/spark2-client
echo "spark.executorEnv.JULIA_HOME ${JULIA_HOME}" >> ${SPARK_HOME}/conf/spark-defaults.conf
echo "spark.executorEnv.JULIA_DEPOT_PATH ${JULIA_DEPOT_PATH}" >> ${SPARK_HOME}/conf/spark-defaults.conf
echo "spark.executorEnv.JULIA_VERSION v1.0.2" >> ${SPARK_HOME}/conf/spark-defaults.conf
for cusr in "${users[@]}"; do
   echo " Adding vars for ser ${cusr}"
   echo "" >> /home/${cusr}/.bashrc
   echo "export MVN_HOME=/usr/local/apache-maven-3.2.2" >> /home/${cusr}/.bashrc
   echo "export PATH=${PATH}:${MVN_HOME}/bin:${JULIA_HOME}" >> /home/${cusr}/.bashrc   
   echo "export YARN_CONF_DIR=/etc/hadoop/conf" >> /home/${cusr}/.bashrc
   echo "export JULIA_HOME=${JULIA_HOME}" >> /home/${cusr}/.bashrc
   echo "export JULIA_DEPOT_PATH=${JULIA_DEPOT_PATH}" >> /home/${cusr}/.bashrc   
   echo "source ${SPARK_HOME}/bin/load-spark-env.sh" >> /home/${cusr}/.bashrc 
   # Set Package folder permissions   
   setfacl -R -m u:${cusr}:rwx ${JULIA_DEPOT_PATH};  
done
