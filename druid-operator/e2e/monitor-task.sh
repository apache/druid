#!/bin/bash
#!/bin/sh
set -e 
echo "---------------"
echo "Checking the status of running job $JOB_ID ..."
for (( i=0; i<=9; i++ ))
do  
    sleep 60
    STAT=`kubectl get job  $JOB_ID --template={{.status.succeeded}}`
    if  [ "$STAT" == "<no value>" ]
    then
        echo "Seems to be in progress ..."
    elif [ $STAT == 1 ]
    then
        echo "Job completed Successfully !!!"
        break
    fi
    if [ $i == 9 ]
    then 
        echo "================"
        echo "Task Timeout ..."
        echo "FAILED EXITING !!!"
        echo "================"
        exit 1
    fi
done
