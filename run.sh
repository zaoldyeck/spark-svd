#!/usr/bin/env bash
domain=hadoop@ec2-52-192-213-179.ap-northeast-1.compute.amazonaws.com #pubgame
ssh -i ~/pubgame.pem ${domain} \
'
screen
spark-submit  --driver-memory 2G \
              --total-executor-cores 120 \
              --num-executors 15 \
              --executor-cores 8 \
              --executor-memory 10G \
              --class Main ./spark-model_2.10-1.0.jar \
              --driver-java-options "-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremotrmi.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=ec2-52-193-124-210.ap-northeast-1.compute.amazonaws.com"

'
exit

domain=vincent@ec2-54-65-230-104.ap-northeast-1.compute.amazonaws.com
ssh ${domain}
nohup spark-submit --class Main ./spark-svd_2.11-1.0.jar 2>log &
spark-submit --class Main ./spark-svd_2.11-1.0.jar
nohup spark-submit --queue longrun --class Main ./spark-svd_2.11-1.0.jar 2>log &