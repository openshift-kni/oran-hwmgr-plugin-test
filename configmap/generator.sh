#!/bin/bash
#

PROG=$(basename "$0")
declare -A PROFILES=()

USERNAME_BASE64=$(echo -n "admin" | base64)
PASSWORD_BASE64=$(echo -n "mypass" | base64)

function usage {
    cat <<EOF
Usage: ${PROG} ...
Parameters:
    --profile <name:prefix:size>

Example:

${0} --profile profile-spr-single-processor-64G:dummy-sp-64g:5 --profile profile-spr-dual-processor-128G:dummy-dp-128g:3

EOF
    exit 1
}

function header {
    cat <<EOF
kind: ConfigMap
apiVersion: v1
metadata:
  name: nodelist
  namespace: oran-hwmgr-plugin-test
data:
  resources: |
EOF
}

function hwprofiles {
    echo "    hwprofiles:"
    mapfile -t sorted_profiles < <( IFS=$'\n'; sort -u <<<"${!PROFILES[*]}" )
    for profile in "${sorted_profiles[@]}"; do
        echo "      - ${profile}"
    done
}

function nodes {
    echo "    nodes:"
    group=0
    mapfile -t sorted_profiles < <( IFS=$'\n'; sort -u <<<"${!PROFILES[*]}" )
    for profile in "${sorted_profiles[@]}"; do
        value="${PROFILES[${profile}]}"
        prefix=$(echo "${value}" | awk -F: '{print $1}')
        size=$(echo "${value}" | awk -F: '{print $2}')
        group=$((group+1))

        for ((i=0;i<${size};i++)); do
            nodename=${prefix}-${i}
            ip="192.168.${group}.${i}"
            mac=$(printf "c6:b6:13:a0:%02x:%02x" ${group} ${i})

            cat <<EOF
      ${nodename}:
        hwprofile: ${profile}
        bmc:
          address: "idrac-virtualmedia+https://${ip}/redfish/v1/Systems/System.Embedded.1"
          username-base64: ${USERNAME_BASE64}
          password-base64: ${PASSWORD_BASE64}
        interfaces:
          - name: eth0
            label: bootable-interface
            macAddress: "${mac}"
        hostname: "${nodename}.localhost"
EOF
        done
    done
}

#
# Process cmdline arguments
#

longopts=(
    "help"
    "profile:"
)

longopts_str=$(IFS=,; echo "${longopts[*]}")

if ! OPTS=$(getopt -o "hp:" --long "${longopts_str}" --name "$0" -- "$@"); then
    usage
    exit 1
fi

eval set -- "${OPTS}"

while :; do
    case "$1" in
        -p|--profile)
            value="$2"
            name=$(echo "${value}" | awk -F: '{print $1}')
            prefix=$(echo "${value}" | awk -F: '{print $2}')
            size=$(echo "${value}" | awk -F: '{print $3}')
            PROFILES+=(["${name}"]="${prefix}:${size}")
            shift 2
            ;;
        --)
            shift
            break                                                                                                                                                                              ;;
        -h|--help)
            usage
            ;;
        *)
            usage
            ;;
    esac
done

if [ "${#PROFILES[@]}" -eq 0 ]; then
    usage
fi

header
hwprofiles
nodes

