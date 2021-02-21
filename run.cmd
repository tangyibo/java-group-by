::	
:: Author: tang
:: Date: 2021-02-21
::	
@echo off
title java-group-by
setlocal enabledelayedexpansion
cls

::java虚拟机启动参数
set MAVEN_OPTS=-server -Xms6g -Xmx6g -Xmn512m -XX:+DisableExplicitGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Djava.awt.headless=true -Dfile.encoding=UTF-8

echo "Clean Project ..."
call mvn clean -f pom.xml

echo "Test Project ..."
call mvn test -f pom.xml

:exit
pause