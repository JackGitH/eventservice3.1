#!/bin/bash
rm eventservice
sleep 1
go build service.go
sleep 4
mv service eventservice
sleep 1
rm nohup.out
rm loggings/eventserver.log
echo "success"
sleep 1
nohup ./eventservice &
