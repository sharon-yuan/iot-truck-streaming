#!/bin/bash

wget http://apache.cs.utah.edu/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
mv apache-maven-3.3.9-bin.tar.gz /tmp
tar xvzf /tmp/apache-maven-*-bin.tar.gz -C /root
mv /root/apache-maven* /root/maven

echo 'M2_HOME=/root/maven' >> ~/.bashrc
echo 'M2=$M2_HOME/bin' >> ~/.bashrc
echo 'PATH=$PATH:$M2' >> ~/.bashrc
