/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package volume

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/scheme"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/reference"
	"k8s.io/utils/ptr"
)

const (
	// AnnBindCompleted Annotation applies to PVCs. It indicates that the lifecycle
	// of the PVC has passed through the initial setup. This information changes how
	// we interpret some observations of the state of the objects. Value of this
	// Annotation does not matter.
	AnnBindCompleted = "pv.kubernetes.io/bind-completed"

	// AnnBoundByController annotation applies to PVs and PVCs. It indicates that
	// the binding (PV->PVC or PVC->PV) was installed by the controller. The
	// absence of this annotation means the binding was done by the user (i.e.
	// pre-bound). Value of this annotation does not matter.
	// External PV binders must bind PV the same way as PV controller, otherwise PV
	// controller may not handle it correctly.
	AnnBoundByController = "pv.kubernetes.io/bound-by-controller"

	// AnnSelectedNode annotation is added to a PVC that has been triggered by scheduler to
	// be dynamically provisioned. Its value is the name of the selected node.
	AnnSelectedNode = "volume.kubernetes.io/selected-node"

	// NotSupportedProvisioner is a special provisioner name which can be set
	// in storage class to indicate dynamic provisioning is not supported by
	// the storage.
	NotSupportedProvisioner = "kubernetes.io/no-provisioner"

	// AnnDynamicallyProvisioned annotation is added to a PV that has been dynamically provisioned by
	// Kubernetes. Its value is name of volume plugin that created the volume.
	// It serves both user (to show where a PV comes from) and Kubernetes (to
	// recognize dynamically provisioned PVs in its decisions).
	AnnDynamicallyProvisioned = "pv.kubernetes.io/provisioned-by"

	// AnnMigratedTo annotation is added to a PVC and PV that is supposed to be
	// dynamically provisioned/deleted by by its corresponding CSI driver
	// through the CSIMigration feature flags. When this annotation is set the
	// Kubernetes components will "stand-down" and the external-provisioner will
	// act on the objects
	AnnMigratedTo = "pv.kubernetes.io/migrated-to"

	// AnnStorageProvisioner annotation is added to a PVC that is supposed to be dynamically
	// provisioned. Its value is name of volume plugin that is supposed to provision
	// a volume for this PVC.
	// TODO: remove beta anno once deprecation period ends
	AnnStorageProvisioner     = "volume.kubernetes.io/storage-provisioner"
	AnnBetaStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"

	//PVDeletionProtectionFinalizer is the finalizer added by the external-provisioner on the PV
	PVDeletionProtectionFinalizer = "external-provisioner.volume.kubernetes.io/finalizer"

	// PVDeletionInTreeProtectionFinalizer is the finalizer added to protect PV deletion for in-tree volumes.
	PVDeletionInTreeProtectionFinalizer = "kubernetes.io/pv-controller"
)

// IsDelayBindingProvisioning checks if claim provisioning with selected-node annotation
func IsDelayBindingProvisioning(claim *v1.PersistentVolumeClaim) bool {
	// When feature VolumeScheduling enabled,
	// Scheduler signal to the PV controller to start dynamic
	// provisioning by setting the "AnnSelectedNode" annotation
	// in the PVC
	_, ok := claim.Annotations[AnnSelectedNode]
	return ok
}

// IsDelayBindingMode checks if claim is in delay binding mode.
// PVC（Persistent Volume Claim）的延迟绑定模式（Delay Binding Mode）是一种特性，
// 允许用户在创建 PVC 时不立即绑定到特定的 PV（Persistent Volume）。
// 而是可以在 PVC 满足特定条件时再进行绑定。
func IsDelayBindingMode(claim *v1.PersistentVolumeClaim, classLister storagelisters.StorageClassLister) (bool, error) {
	className := GetPersistentVolumeClaimClass(claim)
	if className == "" {
		return false, nil
	}

	class, err := classLister.Get(className)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	if class.VolumeBindingMode == nil {
		return false, fmt.Errorf("VolumeBindingMode not set for StorageClass %q", className)
	}

	return *class.VolumeBindingMode == storage.VolumeBindingWaitForFirstConsumer, nil
}

// GetBindVolumeToClaim returns a new volume which is bound to given claim. In
// addition, it returns a bool which indicates whether we made modification on
// original volume.
func GetBindVolumeToClaim(volume *v1.PersistentVolume, claim *v1.PersistentVolumeClaim) (*v1.PersistentVolume, bool, error) {
	dirty := false

	// Check if the volume was already bound (either by user or by controller)
	shouldSetBoundByController := false
	if !IsVolumeBoundToClaim(volume, claim) {
		shouldSetBoundByController = true
	}

	// The volume from method args can be pointing to watcher cache. We must not
	// modify these, therefore create a copy.
	volumeClone := volume.DeepCopy()

	// Bind the volume to the claim if it is not bound yet
	if volume.Spec.ClaimRef == nil ||
		volume.Spec.ClaimRef.Name != claim.Name ||
		volume.Spec.ClaimRef.Namespace != claim.Namespace ||
		volume.Spec.ClaimRef.UID != claim.UID {

		claimRef, err := reference.GetReference(scheme.Scheme, claim)
		if err != nil {
			return nil, false, fmt.Errorf("unexpected error getting claim reference: %w", err)
		}
		volumeClone.Spec.ClaimRef = claimRef
		dirty = true
	}

	// Set AnnBoundByController if it is not set yet
	if shouldSetBoundByController && !metav1.HasAnnotation(volumeClone.ObjectMeta, AnnBoundByController) {
		metav1.SetMetaDataAnnotation(&volumeClone.ObjectMeta, AnnBoundByController, "yes")
		dirty = true
	}

	return volumeClone, dirty, nil
}

// IsVolumeBoundToClaim returns true, if given volume is pre-bound or bound
// to specific claim. Both claim.Name and claim.Namespace must be equal.
// If claim.UID is present in volume.Spec.ClaimRef, it must be equal too.
// 函数 IsVolumeBoundToClaim 的目的是检查给定的 PersistentVolume（PV）是否已经绑定或预绑定到指定的 PersistentVolumeClaim（PVC）。
// 函数主要用于判断 PV 和 PVC 之间的绑定关系是否匹配，以确保存储卷调度和使用的正确性。
func IsVolumeBoundToClaim(volume *v1.PersistentVolume, claim *v1.PersistentVolumeClaim) bool {
	if volume.Spec.ClaimRef == nil {
		return false
	}
	if claim.Name != volume.Spec.ClaimRef.Name || claim.Namespace != volume.Spec.ClaimRef.Namespace {
		return false
	}
	if volume.Spec.ClaimRef.UID != "" && claim.UID != volume.Spec.ClaimRef.UID {
		return false
	}
	return true
}

// FindMatchingVolume goes through the list of volumes to find the best matching volume
// for the claim.
//
// This function is used by both the PV controller and scheduler.
//
// delayBinding is true only in the PV controller path.  When set, prebound PVs are still returned
// as a match for the claim, but unbound PVs are skipped.
//
// node is set only in the scheduler path. When set, the PV node affinity is checked against
// the node's labels.
//
// excludedVolumes is only used in the scheduler path, and is needed for evaluating multiple
// unbound PVCs for a single Pod at one time.  As each PVC finds a matching PV, the chosen
// PV needs to be excluded from future matching.
// FindMatchingVolume 函数遍历卷的列表，以找到与声明最匹配的卷。
//
// 该函数同时用于 PV 控制器和调度器。
//
// 当 delayBinding 为 true 时，仅在 PV 控制器路径中设置。在这种情况下，
// 预绑定的 PV 仍然会作为匹配返回，但未绑定的 PV 会被跳过。
//
// node 仅在调度器路径中设置。当设置时，将检查 PV 的节点亲和性与节点标签的匹配。
//
// excludedVolumes 仅在调度器路径中使用，用于同时评估单个 Pod 的多个未绑定 PVC。
// 当每个 PVC 找到一个匹配的 PV 时，所选择的 PV 需要从未来的匹配中排除。
func FindMatchingVolume(
	claim *v1.PersistentVolumeClaim,
	volumes []*v1.PersistentVolume,
	node *v1.Node,
	excludedVolumes map[string]*v1.PersistentVolume,
	delayBinding bool,
	vacEnabled bool) (*v1.PersistentVolume, error) {

	if !vacEnabled {
		claimVAC := ptr.Deref(claim.Spec.VolumeAttributesClassName, "")
		if claimVAC != "" {
			return nil, fmt.Errorf("unsupported volumeAttributesClassName is set on claim %s when the feature-gate VolumeAttributesClass is disabled", claimToClaimKey(claim))
		}
	}

	var smallestVolume *v1.PersistentVolume
	var smallestVolumeQty resource.Quantity
	requestedQty := claim.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	requestedClass := GetPersistentVolumeClaimClass(claim)

	var selector labels.Selector
	if claim.Spec.Selector != nil {
		// 拿到 selector
		internalSelector, err := metav1.LabelSelectorAsSelector(claim.Spec.Selector)
		if err != nil {
			return nil, fmt.Errorf("error creating internal label selector for claim: %v: %v", claimToClaimKey(claim), err)
		}
		selector = internalSelector
	}

	// Go through all available volumes with two goals:
	// - find a volume that is either pre-bound by user or dynamically
	//   provisioned for this claim. Because of this we need to loop through
	//   all volumes.
	// - find the smallest matching one if there is no volume pre-bound to
	//   the claim.
	// 遍历所有可用卷，目标有两个：
	// - 找到一个要么是用户预绑定的卷，要么是为该声明动态创建的卷。为此，我们需要遍历所有卷。
	// - 如果没有卷预绑定到该声明，则找到最小的匹配卷。
	for _, volume := range volumes {
		if _, ok := excludedVolumes[volume.Name]; ok {
			// Skip volumes in the excluded list
			// 跳过排除列表中的卷
			continue
		}
		if volume.Spec.ClaimRef != nil && !IsVolumeBoundToClaim(volume, claim) {
			// 跳过已经绑定了并且绑定的不是这个 pvc 的
			continue
		}

		volumeQty := volume.Spec.Capacity[v1.ResourceStorage]
		if volumeQty.Cmp(requestedQty) < 0 {
			// 如果requestedQty 大于 volumeQty，跳过
			continue
		}
		// filter out mismatching volumeModes
		if CheckVolumeModeMismatches(&claim.Spec, &volume.Spec) {
			continue
		}

		claimVAC := ptr.Deref(claim.Spec.VolumeAttributesClassName, "")
		volumeVAC := ptr.Deref(volume.Spec.VolumeAttributesClassName, "")

		// filter out mismatching volumeAttributesClassName
		if vacEnabled && claimVAC != volumeVAC {
			continue
		}
		if !vacEnabled && volumeVAC != "" {
			// when the feature gate is disabled, the PV object has VAC set, then we should not bind at all.
			continue
		}

		// check if PV's DeletionTimeStamp is set, if so, skip this volume.
		// 跳过设置了删除时间戳的PV
		if volume.ObjectMeta.DeletionTimestamp != nil {
			continue
		}

		nodeAffinityValid := true
		if node != nil {
			// Scheduler path, check that the PV NodeAffinity
			// is satisfied by the node
			// CheckNodeAffinity is the most expensive call in this loop.
			// We should check cheaper conditions first or consider optimizing this function.
			// 调度器路径，检查 PV 的 NodeAffinity 是否被节点满足。
			// CheckNodeAffinity 是这个循环中开销最大的调用。
			// 我们应该先检查更便宜的条件，或者考虑优化这个函数。
			err := CheckNodeAffinity(volume, node.Labels)
			if err != nil {
				nodeAffinityValid = false
			}
		}

		if IsVolumeBoundToClaim(volume, claim) {
			// If PV node affinity is invalid, return no match.
			// This means the prebound PV (and therefore PVC)
			// is not suitable for this node.
			if !nodeAffinityValid {
				return nil, nil
			}
			// 返回绑定的pv
			return volume, nil
		}

		// 没绑定上
		if node == nil && delayBinding {
			// PV controller does not bind this claim.
			// Scheduler will handle binding unbound volumes
			// Scheduler path will have node != nil
			continue
		}

		// filter out:
		// - volumes in non-available phase
		// - volumes whose labels don't match the claim's selector, if specified
		// - volumes in Class that is not requested
		// - volumes whose NodeAffinity does not match the node
		if volume.Status.Phase != v1.VolumeAvailable {
			// We ignore volumes in non-available phase, because volumes that
			// satisfies matching criteria will be updated to available, binding
			// them now has high chance of encountering unnecessary failures
			// due to API conflicts.
			continue
		} else if selector != nil && !selector.Matches(labels.Set(volume.Labels)) {
			continue
		}
		if GetPersistentVolumeClass(volume) != requestedClass {
			continue
		}
		if !nodeAffinityValid {
			continue
		}

		if node != nil {
			// Scheduler path
			// Check that the access modes match
			if !CheckAccessModes(claim, volume) {
				continue
			}
		}

		if smallestVolume == nil || smallestVolumeQty.Cmp(volumeQty) > 0 {
			smallestVolume = volume
			smallestVolumeQty = volumeQty
		}
	}

	if smallestVolume != nil {
		// Found a matching volume
		return smallestVolume, nil
	}

	return nil, nil
}

// CheckVolumeModeMismatches is a convenience method that checks volumeMode for PersistentVolume
// and PersistentVolumeClaims
func CheckVolumeModeMismatches(pvcSpec *v1.PersistentVolumeClaimSpec, pvSpec *v1.PersistentVolumeSpec) bool {
	// In HA upgrades, we cannot guarantee that the apiserver is on a version >= controller-manager.
	// So we default a nil volumeMode to filesystem
	// 在高可用（HA）升级中，我们无法保证 apiserver 的版本大于或等于 controller-manager。
	// 因此，我们将 nil 的 volumeMode 默认为 filesystem。
	requestedVolumeMode := v1.PersistentVolumeFilesystem
	if pvcSpec.VolumeMode != nil {
		requestedVolumeMode = *pvcSpec.VolumeMode
	}
	pvVolumeMode := v1.PersistentVolumeFilesystem
	if pvSpec.VolumeMode != nil {
		pvVolumeMode = *pvSpec.VolumeMode
	}
	return requestedVolumeMode != pvVolumeMode
}

// CheckAccessModes returns true if PV satisfies all the PVC's requested AccessModes
func CheckAccessModes(claim *v1.PersistentVolumeClaim, volume *v1.PersistentVolume) bool {
	pvModesMap := map[v1.PersistentVolumeAccessMode]bool{}
	for _, mode := range volume.Spec.AccessModes {
		pvModesMap[mode] = true
	}

	for _, mode := range claim.Spec.AccessModes {
		_, ok := pvModesMap[mode]
		if !ok {
			return false
		}
	}
	return true
}

func claimToClaimKey(claim *v1.PersistentVolumeClaim) string {
	return fmt.Sprintf("%s/%s", claim.Namespace, claim.Name)
}
