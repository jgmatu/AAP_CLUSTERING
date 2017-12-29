#!/bin/bash

# This script is execute on launch instance like root user...
# El script crea la instancia configurada para lanzar el programa
# en modo cluster.
#
# Las claves de AWS no pueden ser publicas en un bucket o internet.
# Se debe tener cuidado con la administración de las claves de acceso.
home=/home/ubuntu

apt update && apt -y install awscli

export AWS_ACCESS_KEY_ID=AKIAJ27ZWF3SMX7RLV3A
export AWS_SECRET_ACCESS_KEY=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

aws s3 sync s3://datamipsfiles $home

mv $home/AAP.pem $home/.ssh/id_rsa

cat $home/.ssh/authorized_keys > $home/.ssh/id_rsa.pub

# Perisions details... shell execute with root user....
chmod -R 400 $home/.ssh/
chmod 700 $home/.ssh/

chown -R ubuntu $home/.ssh/
chown -R ubuntu $home/benfort

echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config
/etc/init.d/ssh restart

apt -y install make
apt -y install libopenmpi-dev libopenmpi1.10

cd $home/benfort
make

crontab $home/benfort/mycron
