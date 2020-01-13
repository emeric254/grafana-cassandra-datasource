#!/bin/bash

cd "$(dirname "$0")"

CONTAINER_NAME='my-cassandra-gateway-container'
IMAGE_NAME='my-cassandra-gateway'
NETWORK_NAME='MY_NETWORK'

function network_available {
    docker network inspect $NETWORK_NAME > /dev/null 2> /dev/null
    if [ $? -ne 0 ]
    then
        docker network create --driver bridge $NETWORK_NAME
    fi
}

function remove {
    docker rm $CONTAINER_NAME
    docker rmi $IMAGE_NAME
}

function build {
    docker build -t $IMAGE_NAME .
}

function logs {
    docker logs $CONTAINER_NAME $@
}

function start {
    network_available

    if [[ -n $2 ]]
    then
        STATIC_FILES_PATH=$2
    else
        STATIC_FILES_PATH='/var/lib/cassandra-gateway'
    fi

    mkdir -p $STATIC_FILES_PATH

    docker run --name $CONTAINER_NAME --rm -v $STATIC_FILES_PATH:/var/lib/cassandra-gateway -p 8080:80 --network $NETWORK_NAME -d $IMAGE_NAME
}

function stop {
    docker kill $CONTAINER_NAME
}

function restart {
    network_available
    docker restart $CONTAINER_NAME
}

function kill {
    docker kill $CONTAINER_NAME
}

function help {
    echo "usage: " $0 "<action to perform> [optional additional parameters]"
    cat <<EOF
Parameters:
    'help': show this help
    'start': start services, you can specify static file folder
        - `<static file folder>` (optional), default is "/var/lib/cassandra-gateway"
    'stop': stop services
    'kill': kill services
    'restart': stop and start again services
    'build': create service images
    'remove': remove service images
    'logs': output service logs
        - all `docker logs` parameters are available (optionals)
EOF
}

if [ $# -ge 1 ]
then
    case $1 in
        'logs')
            logs $@
        ;;
        'build')
            build
        ;;
        'start')
            start $@
        ;;
        'restart')
            restart
        ;;
        'stop')
            stop
        ;;
        'kill')
            kill
        ;;
        'remove')
            remove
        ;;
        * )
            help
        ;;
    esac
    exit
fi

help
exit
