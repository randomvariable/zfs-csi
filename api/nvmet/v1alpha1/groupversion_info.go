// Copyright 2026 Naadir Jeewa
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

// Package v1alpha1 contains the API types for the nvmet.randomvariable.co.uk group.
// +kubebuilder:object:generate=true
// +groupName=nvmet.randomvariable.co.uk
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the API group/version for nvmet export resources.
	GroupVersion = schema.GroupVersion{Group: "nvmet.randomvariable.co.uk", Version: "v1alpha1"}

	// SchemeBuilder registers our types with a runtime.Scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme may be used by consumers (manager, clients) to register the scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, &NVMeExport{}, &NVMeExportList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)

	return nil
}
