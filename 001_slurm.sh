#!/usr/bin/bash
#SBATCH --job-name=update-mongo

datadir=/mnt/data/gaoxiang/raw/xyz
printf -v j "%05d" $SLURM_ARRAY_TASK_ID
fn=$datadir/univ-$j

cd /home/gaoxiang/irms/irms-mongo/
JAVA_OPTS="-Xmx40g" scala target/scala-2.12/DoMongoOperations-assembly-0.0.1-SNAPSHOT.jar
