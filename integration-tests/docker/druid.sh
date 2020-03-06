set -e
SERVICE="$1"

echo "$(date -Is) configuring service $SERVICE"

# We put all the config in /tmp/conf to allow for a
# read-only root filesystem
mkdir -p /tmp/conf/druid

getConfPath() {
    cluster_conf_base=/tmp/conf/druid/cluster
    case "$1" in
    _common) echo $cluster_conf_base/_common ;;
    historical) echo $cluster_conf_base/data/historical ;;
    middleManager) echo $cluster_conf_base/data/middleManager ;;
    coordinator | overlord) echo $cluster_conf_base/master/coordinator-overlord ;;
    broker) echo $cluster_conf_base/query/broker ;;
    router) echo $cluster_conf_base/query/router ;;
    esac
}

COMMON_CONF_DIR=$(getConfPath _common)
SERVICE_CONF_DIR=$(getConfPath ${SERVICE})

mkdir -p $COMMON_CONF_DIR
mkdir -p SERVICE_CONF_DIR
touch $COMMON_CONF_DIR/common.runtime.properties
touch $SERVICE_CONF_DIR/runtime.properties

# Delete the old key (if existing) and append new key=value
setKey() {
    service="$1"
    key="$2"
    value="$3"
    service_conf=$(getConfPath $service)/runtime.properties
    # Delete from all
    sed -ri "/$key=/d" $COMMON_CONF_DIR/common.runtime.properties
    [ -f $service_conf ] && sed -ri "/$key=/d" $service_conf
    [ -f $service_conf ] && echo "$key=$value" >>$service_conf
    [ -f $service_conf ] || echo "$key=$value" >>$COMMON_CONF_DIR/common.runtime.properties

    echo "Setting $key=$value in $service_conf"
}

setKey $SERVICE druid.host $(resolveip -s $HOSTNAME)

env | grep ^druid_ | while read evar;
do
    # Can't use IFS='=' to parse since var might have = in it (e.g. password)
    val=$(echo "$evar" | sed -e 's?[^=]*=??')
    var=$(echo "$evar" | sed -e 's?^\([^=]*\)=.*?\1?g' -e 's?_?.?g')
    setKey $SERVICE "$var" "$val"
done

export DRUID_COMMON_CONF_DIR=$COMMON_CONF_DIR
export DRUID_SERVICE_CONF_DIR=$SERVICE_CONF_DIR