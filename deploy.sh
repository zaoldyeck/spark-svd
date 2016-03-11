#!/usr/bin/env bash
domain=vincent@ec2-54-65-230-104.ap-northeast-1.compute.amazonaws.com
sbt +package
scp -P 22 -pr ./target/scala-2.11/spark-svd_2.11-1.0.jar ${domain}:svd