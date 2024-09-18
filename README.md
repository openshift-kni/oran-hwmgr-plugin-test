# oran-hwmgr-plugin-test

The Test Hardware Manager Plugin for O-Cloud Manager (Test Plugin) provides a test mechanism to mock the interactions
between the O-Cloud Manager and a generic hardware manager.

## Overview

The Test Plugin monitors its own namespace for NodePool CRs. When a NodePool CR is created in the Test Plugin namespace,
it will process the CR and checks the (fake) managed resources to find free (fake) nodes to satisfy the NodePool
request. These managed resources are defined in a `nodelist` configmap, defined in the Test Plugin namespace, which
provides a list of hardware profile names and defines the set of managed nodes. See
[configmap/example-nodelist.yaml](configmap/example-nodelist.yaml) for an example `nodelist` configmap. In addition, the
[configmap/generator.sh](configmap/generator.sh) script can be used to generate a `nodelist` configmap.

The `nodelist` configmap is also used by the Test Plugin to track node allocations. As free nodes are allocated to a
NodePool request, these are tracked in the `allocations` field in the configmap and a Node CR is created by the Test
Plugin, setting the node properties as defined in the configmap.

In addition, the Test Plugin will create a `Secret` in its own namespace for each node it allocates, named
`<nodename>-bmc-secret`.

When a NodePool CR is deleted, the Test Plugin is triggered by a finalizer it added to the CR. In processing the
deletion, it will delete any Node CRs that have been allocated for the NodePool and the corresponding bmc-secret, then
free the node(s) in the `nodelist` configmap.

## Testing

### Install O-Cloud Manager

The Test Plugin uses the NodePool and Node CRDs that are defined by the O-Cloud Manager. Please consult the
documentation in the [openshift-kni/oran-o2ims](https://github.com/openshift-kni/oran-o2ims) repository for information
on deploying the O-Cloud Manager to your cluster.

To run the Test Plugin as a standalone without the O-Cloud Manager, the CRDs can be installed from the repo by running:
`make install`

### Deploy Test Plugin

The Test Plugin can be deployed to your cluster by first building the image and then running the `deploy` make target:

```console
$ make IMAGE_TAG_BASE=quay.io/$USER/oran-hwmgr-plugin-test VERSION=latest docker-build docker-push
$ make IMAGE_TAG_BASE=quay.io/$USER/oran-hwmgr-plugin-test VERSION=latest deploy
```

To watch the Test Plugin logs, run the following command:

```console
oc logs -n oran-hwmgr-plugin-test -l control-plane=controller-manager -f
```

The [configmap/generator.sh](configmap/generator.sh) script can be used to generate a `nodelist` configmap with
user-defined hardware profiles and any number of nodes.

Example test NodePool CRs can be found in the [examples](examples) folder.

```console
$ ./configmap/generator.sh \
    --profile profile-spr-single-processor-64G:dummy-sp-64g:5 \
	--profile profile-spr-dual-processor-128G:dummy-dp-128g:3 \
	| oc create -f -
configmap/nodelist created

$ oc create -f examples/np1.yaml
nodepool.hardwaremanagement.oran.openshift.io/np1 created

$ oc get nodepools.hardwaremanagement.oran.openshift.io -n oran-hwmgr-plugin-test np1 -o yaml
apiVersion: hardwaremanagement.oran.openshift.io/v1alpha1
kind: NodePool
metadata:
  creationTimestamp: "2024-09-18T17:29:00Z"
  finalizers:
  - oran-hwmgr-plugin-test.oran.openshift.io/nodepool-finalizer
  generation: 1
  name: np1
  namespace: oran-hwmgr-plugin-test
  resourceVersion: "15851"
  uid: 00be582c-3026-4421-bb92-b394415cff6b
spec:
  cloudID: testcloud-1
  location: ottawa
  nodeGroup:
  - hwProfile: profile-spr-single-processor-64G
    name: master
    size: 1
  - hwProfile: profile-spr-dual-processor-128G
    name: worker
    size: 0
  site: building-1
status:
  conditions:
  - lastTransitionTime: "2024-09-18T17:29:21Z"
    message: Created
    reason: Completed
    status: "True"
    type: Provisioned
  properties:
    nodeNames:
    - dummy-sp-64g-1

$ oc get nodes.hardwaremanagement.oran.openshift.io -n oran-hwmgr-plugin-test dummy-sp-64g-1 -o yaml
apiVersion: hardwaremanagement.oran.openshift.io/v1alpha1
kind: Node
metadata:
  creationTimestamp: "2024-09-18T17:29:11Z"
  generation: 1
  name: dummy-sp-64g-1
  namespace: oran-hwmgr-plugin-test
  resourceVersion: "15831"
  uid: a0021f5a-34bc-4e5f-95c4-8372f4ce3fa3
spec:
  groupName: master
  hwProfile: profile-spr-single-processor-64G
  nodePool: testcloud-1
status:
  bmc:
    address: idrac-virtualmedia+https://192.168.2.1/redfish/v1/Systems/System.Embedded.1
    credentialsName: dummy-sp-64g-1-bmc-secret
  conditions:
  - lastTransitionTime: "2024-09-18T17:29:11Z"
    message: Provisioned
    reason: Completed
    status: "True"
    type: Provisioned
  hostname: dummy-sp-64g-1.localhost
  interfaces:
  - label: bootable-interface
    macAddress: c6:b6:13:a0:02:01
    name: eth0

$ oc get configmap -n oran-hwmgr-plugin-test nodelist -o yaml
apiVersion: v1
data:
  allocations: |
    clouds:
    - cloudID: testcloud-1
      nodegroups:
        master:
        - dummy-sp-64g-1
  resources: |
    hwprofiles:
      - profile-spr-dual-processor-128G
      - profile-spr-single-processor-64G
    nodes:
      dummy-dp-128g-0:
        hwprofile: profile-spr-dual-processor-128G
        bmc:
          address: "idrac-virtualmedia+https://192.168.1.0/redfish/v1/Systems/System.Embedded.1"
          username-base64: YWRtaW4=
          password-base64: bXlwYXNz
        interfaces:
          - name: eth0
            label: bootable-interface
            macAddress: "c6:b6:13:a0:01:00"
        hostname: "dummy-dp-128g-0.localhost"
      dummy-dp-128g-1:
        hwprofile: profile-spr-dual-processor-128G
        bmc:
          address: "idrac-virtualmedia+https://192.168.1.1/redfish/v1/Systems/System.Embedded.1"
          username-base64: YWRtaW4=
          password-base64: bXlwYXNz
        interfaces:
          - name: eth0
            label: bootable-interface
            macAddress: "c6:b6:13:a0:01:01"
        hostname: "dummy-dp-128g-1.localhost"
      dummy-dp-128g-2:
        hwprofile: profile-spr-dual-processor-128G
        bmc:
          address: "idrac-virtualmedia+https://192.168.1.2/redfish/v1/Systems/System.Embedded.1"
          username-base64: YWRtaW4=
          password-base64: bXlwYXNz
        interfaces:
          - name: eth0
            label: bootable-interface
            macAddress: "c6:b6:13:a0:01:02"
        hostname: "dummy-dp-128g-2.localhost"
      dummy-sp-64g-0:
        hwprofile: profile-spr-single-processor-64G
        bmc:
          address: "idrac-virtualmedia+https://192.168.2.0/redfish/v1/Systems/System.Embedded.1"
          username-base64: YWRtaW4=
          password-base64: bXlwYXNz
        interfaces:
          - name: eth0
            label: bootable-interface
            macAddress: "c6:b6:13:a0:02:00"
        hostname: "dummy-sp-64g-0.localhost"
      dummy-sp-64g-1:
        hwprofile: profile-spr-single-processor-64G
        bmc:
          address: "idrac-virtualmedia+https://192.168.2.1/redfish/v1/Systems/System.Embedded.1"
          username-base64: YWRtaW4=
          password-base64: bXlwYXNz
        interfaces:
          - name: eth0
            label: bootable-interface
            macAddress: "c6:b6:13:a0:02:01"
        hostname: "dummy-sp-64g-1.localhost"
      dummy-sp-64g-2:
        hwprofile: profile-spr-single-processor-64G
        bmc:
          address: "idrac-virtualmedia+https://192.168.2.2/redfish/v1/Systems/System.Embedded.1"
          username-base64: YWRtaW4=
          password-base64: bXlwYXNz
        interfaces:
          - name: eth0
            label: bootable-interface
            macAddress: "c6:b6:13:a0:02:02"
        hostname: "dummy-sp-64g-2.localhost"
      dummy-sp-64g-3:
        hwprofile: profile-spr-single-processor-64G
        bmc:
          address: "idrac-virtualmedia+https://192.168.2.3/redfish/v1/Systems/System.Embedded.1"
          username-base64: YWRtaW4=
          password-base64: bXlwYXNz
        interfaces:
          - name: eth0
            label: bootable-interface
            macAddress: "c6:b6:13:a0:02:03"
        hostname: "dummy-sp-64g-3.localhost"
      dummy-sp-64g-4:
        hwprofile: profile-spr-single-processor-64G
        bmc:
          address: "idrac-virtualmedia+https://192.168.2.4/redfish/v1/Systems/System.Embedded.1"
          username-base64: YWRtaW4=
          password-base64: bXlwYXNz
        interfaces:
          - name: eth0
            label: bootable-interface
            macAddress: "c6:b6:13:a0:02:04"
        hostname: "dummy-sp-64g-4.localhost"
kind: ConfigMap
metadata:
  creationTimestamp: "2024-09-18T17:28:38Z"
  name: nodelist
  namespace: oran-hwmgr-plugin-test
  resourceVersion: "15829"
  uid: e6dfe91c-0713-46d2-b3e5-b883c3d8b8c5

```


