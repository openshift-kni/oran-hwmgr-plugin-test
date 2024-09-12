package service

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/openshift-kni/oran-hwmgr-plugin-test/internal/controller/utils"
	hwmgmtv1alpha1 "github.com/openshift-kni/oran-o2ims/api/hardwaremanagement/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

// Struct definitions for the nodelist configmap
type cmBmcInfo struct {
	Address        string `json:"address,omitempty"`
	UsernameBase64 string `json:"username-base64,omitempty"`
	PasswordBase64 string `json:"password-base64,omitempty"`
}

type cmNodeInfo struct {
	HwProfile      string     `json:"hwprofile" yaml:"hwprofile"`
	BMC            *cmBmcInfo `json:"bmc,omitempty"`
	BootMACAddress string     `json:"bootMACAddress,omitempty"`
	Hostname       string     `json:"hostname,omitempty"`
}

type cmResources struct {
	HwProfiles []string              `json:"hwprofiles" yaml:"hwprofiles"`
	Nodes      map[string]cmNodeInfo `json:"nodes" yaml:"nodes"`
}

type cmAllocatedCloud struct {
	CloudID    string              `json:"cloudID" yaml:"cloudID"`
	Nodegroups map[string][]string `json:"nodegroups" yaml:"nodegroups"`
}

type cmAllocations struct {
	Clouds []cmAllocatedCloud `json:"clouds" yaml:"clouds"`
}

const (
	resourcesKey   = "resources"
	allocationsKey = "allocations"
	cmName         = "nodelist"
)

// Define the HwMgrService structures
type HwMgrServiceBuilder struct {
	client.Client
	logger *slog.Logger
}

type HwMgrService struct {
	client.Client
	logger    *slog.Logger
	namespace string
}

// Functions for creating a new HwMgrService
func NewHwMgrService() *HwMgrServiceBuilder {
	return &HwMgrServiceBuilder{}
}

func (b *HwMgrServiceBuilder) SetClient(
	value client.Client) *HwMgrServiceBuilder {
	b.Client = value
	return b
}

func (b *HwMgrServiceBuilder) SetLogger(
	value *slog.Logger) *HwMgrServiceBuilder {
	b.logger = value
	return b
}

func (b *HwMgrServiceBuilder) Build(ctx context.Context) (
	result *HwMgrService, err error) {
	if b.logger == nil {
		err = errors.New("logger is mandatory")
		return
	}

	service := &HwMgrService{
		Client:    b.Client,
		logger:    b.logger,
		namespace: os.Getenv("MY_POD_NAMESPACE"),
	}

	result = service
	return
}

// getFreeNodesInProfile compares the parsed configmap data to get the list of free nodes for a given hardware profile
func getFreeNodesInProfile(resources cmResources, allocations cmAllocations, profname string) (freenodes []string) {
	inuse := make(map[string]bool)
	for _, cloud := range allocations.Clouds {
		for groupname := range cloud.Nodegroups {
			for _, nodename := range cloud.Nodegroups[groupname] {
				inuse[nodename] = true
			}
		}
	}

	for nodename, node := range resources.Nodes {
		if node.HwProfile != profname {
			continue
		}

		if _, used := inuse[nodename]; !used {
			freenodes = append(freenodes, nodename)
		}
	}

	return
}

// GetCurrentResources parses the nodelist configmap to get the current available and allocated resource lists
func (h *HwMgrService) GetCurrentResources(ctx context.Context) (
	cm *corev1.ConfigMap, resources cmResources, allocations cmAllocations, err error) {
	cm, err = utils.GetConfigmap(ctx, h.Client, cmName, h.namespace)
	if err != nil {
		err = fmt.Errorf("unable to get configmap: %w", err)
		return
	}

	resources, err = utils.ExtractDataFromConfigMap[cmResources](cm, resourcesKey)
	if err != nil {
		err = fmt.Errorf("unable to parse resources from configmap: %w", err)
		return
	}

	allocations, err = utils.ExtractDataFromConfigMap[cmAllocations](cm, allocationsKey)
	if err != nil {
		// Allocated node field may not be present
		h.logger.InfoContext(ctx, "unable to parse allocations from configmap")
		err = nil
	}

	return
}

// ProcessNewNodePool processes a new NodePool CR, verifying that there are enough free resources to satisfy the request
func (h *HwMgrService) ProcessNewNodePool(ctx context.Context, nodepool *hwmgmtv1alpha1.NodePool) error {
	cloudID := nodepool.Spec.CloudID

	h.logger.InfoContext(ctx, "Processing ProcessNewNodePool request:",
		"cloudID", cloudID,
	)

	_, resources, allocations, err := h.GetCurrentResources(ctx)
	if err != nil {
		return fmt.Errorf("unable to get current resources: %w", err)
	}

	for _, nodegroup := range nodepool.Spec.NodeGroup {
		freenodes := getFreeNodesInProfile(resources, allocations, nodegroup.HwProfile)
		if nodegroup.Size > len(freenodes) {
			return fmt.Errorf("not enough free resources in group %s: freenodes=%d", nodegroup.HwProfile, len(freenodes))
		}
	}

	return nil
}

// AllocateNode processes a NodePool CR, allocating a free node for each specified nodegroup as needed
func (h *HwMgrService) AllocateNode(ctx context.Context, nodepool *hwmgmtv1alpha1.NodePool) error {
	cloudID := nodepool.Spec.CloudID

	// Inject a delay before allocating node
	time.Sleep(10 * time.Second)

	cm, resources, allocations, err := h.GetCurrentResources(ctx)
	if err != nil {
		return fmt.Errorf("unable to get current resources: %w", err)
	}

	var cloud *cmAllocatedCloud
	for i, iter := range allocations.Clouds {
		if iter.CloudID == cloudID {
			cloud = &allocations.Clouds[i]
			break
		}
	}
	if cloud == nil {
		// The cloud wasn't found in the list, so create a new entry
		allocations.Clouds = append(allocations.Clouds, cmAllocatedCloud{CloudID: cloudID, Nodegroups: make(map[string][]string)})
		cloud = &allocations.Clouds[len(allocations.Clouds)-1]
	}

	// Check available resources
	for _, nodegroup := range nodepool.Spec.NodeGroup {
		used := cloud.Nodegroups[nodegroup.Name]
		remaining := nodegroup.Size - len(used)
		if remaining <= 0 {
			// This group is allocated
			h.logger.InfoContext(ctx, "nodegroup is fully allocated", "nodegroup", nodegroup.Name)
			continue
		}

		freenodes := getFreeNodesInProfile(resources, allocations, nodegroup.HwProfile)
		if remaining > len(freenodes) {
			return fmt.Errorf("not enough free resources remaining in group %s", nodegroup.HwProfile)
		}

		// Grab the first node
		nodename := freenodes[0]

		nodeinfo, exists := resources.Nodes[nodename]
		if !exists {
			return fmt.Errorf("unable to find nodeinfo for %s", nodename)
		}

		if err := h.CreateBMCSecret(ctx, nodename, nodeinfo.BMC.UsernameBase64, nodeinfo.BMC.PasswordBase64); err != nil {
			return fmt.Errorf("failed to create bmc-secret when allocating node %s: %w", nodename, err)
		}

		cloud.Nodegroups[nodegroup.Name] = append(cloud.Nodegroups[nodegroup.Name], nodename)

		// Update the configmap
		yamlString, err := yaml.Marshal(&allocations)
		if err != nil {
			return fmt.Errorf("unable to marshal allocated data: %w", err)
		}
		cm.Data[allocationsKey] = string(yamlString)
		if err := h.Client.Update(ctx, cm); err != nil {
			return fmt.Errorf("failed to update configmap: %w", err)
		}

		if err := h.CreateNode(ctx, cloudID, nodename, nodegroup.Name, nodegroup.HwProfile); err != nil {
			return fmt.Errorf("failed to create allocated node (%s): %w", nodename, err)
		}

		if err := h.UpdateNodeStatus(ctx, nodename, nodeinfo); err != nil {
			return fmt.Errorf("failed to update node status (%s): %w", nodename, err)
		}
	}

	return nil
}

func bmcSecretName(nodename string) string {
	return fmt.Sprintf("%s-bmc-secret", nodename)
}

// CreateBMCSecret creates the bmc-secret for a node
func (h *HwMgrService) CreateBMCSecret(ctx context.Context, nodename, usernameBase64, passwordBase64 string) error {
	h.logger.InfoContext(ctx, "Creating bmc-secret:", "nodename", nodename)

	secretName := bmcSecretName(nodename)

	username, err := base64.StdEncoding.DecodeString(usernameBase64)
	if err != nil {
		return fmt.Errorf("failed to decode usernameBase64 string (%s) for node %s: %w", usernameBase64, nodename, err)
	}

	password, err := base64.StdEncoding.DecodeString(passwordBase64)
	if err != nil {
		return fmt.Errorf("failed to decode usernameBase64 string (%s) for node %s: %w", passwordBase64, nodename, err)
	}

	bmcSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: h.namespace,
		},
		Data: map[string][]byte{
			"username": username,
			"password": password,
		},
	}

	if err = utils.CreateK8sCR(ctx, h.Client, bmcSecret, nil, utils.UPDATE); err != nil {
		return fmt.Errorf("failed to create bmc-secret for node %s: %w", nodename, err)
	}

	return nil
}

// DeleteBMCSecret deletes the bmc-secret for a node
func (h *HwMgrService) DeleteBMCSecret(ctx context.Context, nodename string) error {
	h.logger.InfoContext(ctx, "Deleting bmc-secret:", "nodename", nodename)

	secretName := bmcSecretName(nodename)

	bmcSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: h.namespace,
		},
	}

	if err := h.Client.Delete(ctx, bmcSecret); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to delete bmc-secret for node %s: %w", nodename, err)
	}

	return nil
}

// CreateNode creates a Node CR with specified attributes
func (h *HwMgrService) CreateNode(ctx context.Context, cloudID, nodename, groupname, hwprofile string) error {

	h.logger.InfoContext(ctx, "Creating node:",
		"cloudID", cloudID,
		"nodegroup name", groupname,
		"nodename", nodename,
	)

	node := &hwmgmtv1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodename,
			Namespace: h.namespace,
		},
		Spec: hwmgmtv1alpha1.NodeSpec{
			NodePool:  cloudID,
			GroupName: groupname,
			HwProfile: hwprofile,
		},
	}

	if err := h.Client.Create(ctx, node); err != nil {
		return fmt.Errorf("failed to create Node: %w", err)
	}

	return nil
}

// UpdateNodeStatus updates a Node CR status field with additional node information from the nodelist configmap
func (h *HwMgrService) UpdateNodeStatus(ctx context.Context, nodename string, info cmNodeInfo) error {

	h.logger.InfoContext(ctx, "Updating node:",
		"nodename", nodename,
	)

	node := &hwmgmtv1alpha1.Node{}

	if err := h.Client.Get(ctx, types.NamespacedName{Name: nodename, Namespace: h.namespace}, node); err != nil {
		return fmt.Errorf("failed to create Node: %w", err)
	}

	h.logger.InfoContext(ctx, "Adding info to node", "nodename", nodename, "info", info)
	node.Status.BMC = &hwmgmtv1alpha1.BMC{
		Address:         info.BMC.Address,
		CredentialsName: bmcSecretName(nodename),
	}
	node.Status.BootMACAddress = info.BootMACAddress
	node.Status.Hostname = info.Hostname

	utils.SetStatusCondition(&node.Status.Conditions,
		hwmgmtv1alpha1.Provisioned,
		hwmgmtv1alpha1.Completed,
		metav1.ConditionTrue,
		"Provisioned")

	if err := utils.UpdateK8sCRStatus(ctx, h.Client, node); err != nil {
		return fmt.Errorf("failed to update status for node %s: %w", nodename, err)
	}

	return nil
}

// DeleteNode deletes a Node CR
func (h *HwMgrService) DeleteNode(ctx context.Context, nodename string) error {

	h.logger.InfoContext(ctx, "Deleting node:",
		"nodename", nodename,
	)

	node := &hwmgmtv1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodename,
			Namespace: h.namespace,
		},
	}

	if err := h.Client.Delete(ctx, node); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to delete Node: %w", err)
	}

	return nil
}

// IsNodeFullyAllocated checks to see if a NodePool CR has been fully allocated
func (h *HwMgrService) IsNodeFullyAllocated(ctx context.Context, nodepool *hwmgmtv1alpha1.NodePool) (bool, error) {
	cloudID := nodepool.Spec.CloudID

	_, resources, allocations, err := h.GetCurrentResources(ctx)
	if err != nil {
		return false, fmt.Errorf("unable to get current resources: %w", err)
	}

	var cloud *cmAllocatedCloud
	for i, iter := range allocations.Clouds {
		if iter.CloudID == cloudID {
			cloud = &allocations.Clouds[i]
			break
		}
	}
	if cloud == nil {
		// Cloud has not been allocated yet
		return false, nil
	}

	// Check allocated resources
	for _, nodegroup := range nodepool.Spec.NodeGroup {
		used := cloud.Nodegroups[nodegroup.Name]
		remaining := nodegroup.Size - len(used)
		if remaining <= 0 {
			// This group is allocated
			h.logger.InfoContext(ctx, "nodegroup is fully allocated", "nodegroup", nodegroup.Name)
			continue
		}

		freenodes := getFreeNodesInProfile(resources, allocations, nodegroup.HwProfile)
		if remaining > len(freenodes) {
			return false, fmt.Errorf("not enough free resources remaining in group %s", nodegroup.HwProfile)
		}

		// Cloud is not fully allocated, and there are resources available
		return false, nil
	}

	return true, nil
}

// GetAllocatedNodes gets a list of nodes allocated for the specified NodePool CR
func (h *HwMgrService) GetAllocatedNodes(ctx context.Context, nodepool *hwmgmtv1alpha1.NodePool) (allocatedNodes []string, err error) {
	cloudID := nodepool.Spec.CloudID

	_, _, allocations, err := h.GetCurrentResources(ctx)
	if err != nil {
		err = fmt.Errorf("unable to get current resources: %w", err)
		return
	}

	var cloud *cmAllocatedCloud
	for i, iter := range allocations.Clouds {
		if iter.CloudID == cloudID {
			cloud = &allocations.Clouds[i]
			break
		}
	}
	if cloud == nil {
		// Cloud has not been allocated yet
		return
	}

	// Get allocated resources
	for _, nodegroup := range nodepool.Spec.NodeGroup {
		allocatedNodes = append(allocatedNodes, cloud.Nodegroups[nodegroup.Name]...)
	}

	slices.Sort(allocatedNodes)
	return
}

// CheckNodePoolProgress checks to see if a NodePool is fully allocated, allocating additional resources as needed
func (h *HwMgrService) CheckNodePoolProgress(ctx context.Context, nodepool *hwmgmtv1alpha1.NodePool) (full bool, err error) {
	cloudID := nodepool.Spec.CloudID

	if full, err = h.IsNodeFullyAllocated(ctx, nodepool); err != nil {
		err = fmt.Errorf("failed to check nodepool allocation: %w", err)
		return
	} else if full {
		// Node is fully allocated
		return
	}

	for _, nodegroup := range nodepool.Spec.NodeGroup {
		h.logger.InfoContext(ctx, "Allocating node for CheckNodePoolProgress request:",
			"cloudID", cloudID,
			"nodegroup name", nodegroup.Name,
		)

		if err = h.AllocateNode(ctx, nodepool); err != nil {
			err = fmt.Errorf("failed to allocate node: %w", err)
			return
		}
	}

	return
}

// ReleaseNodePool frees resources allocated to a NodePool
func (h *HwMgrService) ReleaseNodePool(ctx context.Context, nodepool *hwmgmtv1alpha1.NodePool) error {
	cloudID := nodepool.Spec.CloudID

	h.logger.InfoContext(ctx, "Processing ReleaseNodePool request:",
		"cloudID", cloudID,
	)

	cm, _, allocations, err := h.GetCurrentResources(ctx)
	if err != nil {
		return fmt.Errorf("unable to get current resources: %w", err)
	}

	index := -1
	for i, cloud := range allocations.Clouds {
		if cloud.CloudID == cloudID {
			index = i
			break
		}
	}

	if index == -1 {
		h.logger.InfoContext(ctx, "no allocated nodes found", "cloudID", cloudID)
		return nil
	}

	for groupname := range allocations.Clouds[index].Nodegroups {
		for _, nodename := range allocations.Clouds[index].Nodegroups[groupname] {
			if err := h.DeleteBMCSecret(ctx, nodename); err != nil {
				return fmt.Errorf("failed to delete bmc-secret for %s: %w", nodename, err)
			}

			if err := h.DeleteNode(ctx, nodename); err != nil {
				return fmt.Errorf("failed to delete node %s: %w", nodename, err)
			}
		}
	}

	allocations.Clouds = slices.Delete[[]cmAllocatedCloud](allocations.Clouds, index, index+1)

	// Update the configmap
	yamlString, err := yaml.Marshal(&allocations)
	if err != nil {
		return fmt.Errorf("unable to marshal allocated data: %w", err)
	}
	cm.Data[allocationsKey] = string(yamlString)
	if err := h.Client.Update(ctx, cm); err != nil {
		return fmt.Errorf("failed to update configmap: %w", err)
	}

	return nil
}
