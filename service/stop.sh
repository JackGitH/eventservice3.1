ps -ef | grep eventservice | grep -v grep | awk '{print $2}' | xargs kill
