#!/usr/bin/env bash
#
# Author : tang
# Date : 2021-02-21
#

## java虚拟机启动参数
export MAVEN_OPTS="-server -Xms4g -Xmx4g -Xmn1g -XX:+DisableExplicitGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Djava.awt.headless=true -Dfile.encoding=UTF-8"

echo "Clean Project ..."
mvn clean -f pom.xml

echo "Test Project ..."
mvn test -f pom.xml