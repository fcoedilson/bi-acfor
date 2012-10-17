#!/bin/bash

#echo "APAGANDO ARQUIVOS ANTERIORES DA PASTA /opt/cagece_files/ ..."

#sudo rm -rf /opt/cagece_files/* &

#sleep 5


echo "CONECTANDO EM ftp://ftp.cagece.com.br ....... "
wget ftp://ftp.cagece.com.br/*.txt --user='ACFOR' --password='A@CFO$R' -P /opt/dwacfor/ftp_files/ 2> /tmp/dwacfor_ftp.err

echo "ARQUIVOS DE ftp://ftp.cagece.com.br BAIXADOS ..."

echo "EXECUTANDO ETL ...."
sudo perl /opt/dwacfor/dwacfor_script_etl.pl 2> /tmp/dwacfor_etl.err

echo "ETL EXECUTADO COM SUCESSO !!!"

rm -rf /opt/dwacfor/ftp_files/*
echo "ARQUIVOS BAIXADOS APAGADOS!"

sleep 1
exit 0
