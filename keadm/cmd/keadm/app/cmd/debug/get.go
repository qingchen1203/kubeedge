/*
Copyright 2020 The KubeEdge Authors.

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

package debug

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/astaxie/beego/orm"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/kubectl/pkg/scheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	k8sprinters "k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	"k8s.io/kubernetes/pkg/printers/storage"

	"github.com/kubeedge/beehive/pkg/common/util"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/common/constants"
	"github.com/kubeedge/kubeedge/edge/pkg/common/dbm"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao"
	edgecoreCfg "github.com/kubeedge/kubeedge/pkg/apis/componentconfig/edgecore/v1alpha1"
)

const (
	ResourceTypeAll       = "all"
	ResourceTypePod       = "pod"
	ResourceTypeService   = "service"
	ResourceTypeSecret    = "secret"
	ResourceTypeConfigmap = "configmap"
	ResourceTypeEndpoints = "endpoints"

	OutputFormatWIDE = "wide"
	OutputFormatJSON = "json"
	OutputFormatYAML = "yaml"
)

var (
	debugGetLong = `
Prints a table of the most important information about the specified resource from the local database of the edge node.`
	debugGetExample = `
# List all pod in namespace test
keadm debug get pod -n test

# List a single configmap  with specified NAME
keadm debug get configmap web -n default

# List the complete information of the configmap with the specified name in the yaml output format
keadm debug get configmap web -n default -o yaml

# List the complete information of all available resources of edge nodes using the specified format (default: yaml)
keadm debug get all -o yaml`

	// allowedFormats Currently supports formats such as yaml|json|wide
	allowedFormats = []string{OutputFormatYAML, OutputFormatJSON, OutputFormatWIDE}

	// availableResources Convert flag to currently supports available Resource types in EdgeCore database.
	availableResources = map[string]string{
		"all":        "'pod','service','secret','configmap','endpoints'",
		"po":         "'pod'",
		"pod":        "'pod'",
		"pods":       "'pod'",
		"svc":        "'service'",
		"service":    "'service'",
		"services":   "'service'",
		"secret":     "'secret'",
		"secrets":    "'secret'",
		"cm":         "'configmap'",
		"configmap":  "'configmap'",
		"configmaps": "'configmap'",
		"ep":         "'endpoints'",
		"endpoint":   "'endpoints'",
		"endpoints":  "'endpoints'",
	}
)

const (
	// DefaultErrorExitCode defines exit the code for failed action generally
	DefaultErrorExitCode = 1
)

// NewCmdDebugGet returns keadm debug get command.
func NewCmdDebugGet(out io.Writer, getOption *GetOptions) *cobra.Command {
	if getOption == nil {
		getOption = NewGetOptions()
	}

	cmd := &cobra.Command{
		Use:     "get",
		Short:   "Display one or many resources",
		Long:    debugGetLong,
		Example: debugGetExample,
		Run: func(cmd *cobra.Command, args []string) {
			if err := getOption.Validate(args); err != nil {
				CheckErr(err, fatal)
			}
			if err := getOption.ExecuteGet(args, out); err != nil {
				CheckErr(err, fatal)
			}
		},
	}
	addGetOtherFlags(cmd, getOption)

	return cmd
}

// fatal prints the message if set and then exits.
func fatal(msg string, code int) {
	if len(msg) > 0 {
		// add newline if needed
		if !strings.HasSuffix(msg, "\n") {
			msg += "\n"
		}

		fmt.Fprint(os.Stderr, msg)
	}
	os.Exit(code)
}

// CheckErr formats a given error as a string and calls the passed handleErr
// func with that string and an exit code.
func CheckErr(err error, handleErr func(string, int)) {
	switch err.(type) {
	case nil:
		return
	default:
		handleErr(err.Error(), DefaultErrorExitCode)
	}
}

// GetOptions contains the input to the get command.
type GetOptions struct {
	AllNamespace  bool
	Namespace     string
	LabelSelector string
	DataPath      string

	PrintFlags *PrintFlags
}

// addGetOtherFlags
func addGetOtherFlags(cmd *cobra.Command, getOption *GetOptions) {
	cmd.Flags().StringVarP(&getOption.Namespace, "namespace", "n", getOption.Namespace, "List the requested object(s) in specified namespaces")
	cmd.Flags().StringVarP(getOption.PrintFlags.OutputFormat, "output", "o", *getOption.PrintFlags.OutputFormat, "Indicate the output format. Currently supports formats such as yaml|json|wide")
	cmd.Flags().StringVarP(&getOption.LabelSelector, "selector", "l", getOption.LabelSelector, "Selector (label query) to filter on, supports '=', '==', and '!='.(e.g. -l key1=value1,key2=value2)")
	cmd.Flags().StringVarP(&getOption.DataPath, "input", "i", getOption.DataPath, "Indicate the edge node database path, the default path is \"/var/lib/kubeedge/edgecore.db\"")
	cmd.Flags().BoolVarP(&getOption.AllNamespace, "all-namespaces", "A", getOption.AllNamespace, "List the requested object(s) across all namespaces")
}

// NewGetOptions returns a GetOptions with default EdgeCore database source.
func NewGetOptions() *GetOptions {
	opts := &GetOptions{
		Namespace:  "default",
		DataPath:   edgecoreCfg.DataBaseDataSource,
		PrintFlags: NewGetPrintFlags(),
	}

	return opts
}

// Validate checks the set of flags provided by the user.
func (g *GetOptions) Validate(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("You must specify the type of resource to get. ")
	}
	if !IsAvailableResources(args[0]) {
		return fmt.Errorf("Unrecognized resource type: %v. ", args[0])
	}
	if len(g.DataPath) == 0 {
		fmt.Printf("Not specified the EdgeCore database path, use the default path: %v. ", g.DataPath)
	}
	if !IsFileExist(g.DataPath) {
		return fmt.Errorf("EdgeCore database file %v not exist. ", g.DataPath)
	}

	if err := InitDB(edgecoreCfg.DataBaseDriverName, edgecoreCfg.DataBaseAliasName, g.DataPath); err != nil {
		return fmt.Errorf("Failed to initialize database: %v ", err)
	}
	if len(*g.PrintFlags.OutputFormat) > 0 {
		format := strings.ToLower(*g.PrintFlags.OutputFormat)
		g.PrintFlags.OutputFormat = &format
		if !IsAllowedFormat(*g.PrintFlags.OutputFormat) {
			return fmt.Errorf("Invalid output format: %v, currently supports formats such as yaml|json|wide. ", *g.PrintFlags.OutputFormat)
		}
	}
	if args[0] == ResourceTypeAll && len(args) >= 2 {
		return fmt.Errorf("You must specify only one resource. ")
	}

	return nil
}

// ExecuteGet performs the get operation.
func (g *GetOptions) ExecuteGet(args []string, out io.Writer) error {
	resType := args[0]
	resNames := args[1:]
	//results, err := QueryMetaFromDatabase(g.AllNamespace, g.Namespace, resType, resNames)
	results, err := g.QueryDataFromDatabase(resType, resNames)
	if err != nil {
		return err
	}

	if len(g.LabelSelector) > 0 {
		results, err = FilterSelector(results, g.LabelSelector)
		if err != nil {
			return err
		}
	}

	printer, err := g.PrintFlags.ToPrinter()
	if err != nil {
		return err
	}
	printer, err = printers.NewTypeSetter(scheme.Scheme).WrapToPrinter(printer, nil)

	if len(results) == 0 {
		if _, err := fmt.Fprintf(out, "No resources found in %v namespace.\n", g.Namespace); err != nil {
			return err
		}
		return nil
	}
	if *g.PrintFlags.OutputFormat == "" || *g.PrintFlags.OutputFormat == OutputFormatWIDE {
		return HumanReadablePrint(results, printer, out)
	}

	return JSONYamlPrint(results, printer, out)
}

// JSONYamlPrint Output the data in json|yaml format
func JSONYamlPrint(results []dao.Meta, printer printers.ResourcePrinter, out io.Writer) error {
	var obj runtime.Object
	list := v1.List{
		TypeMeta: metav1.TypeMeta{
			Kind:       "List",
			APIVersion: "v1",
		},
		ListMeta: metav1.ListMeta{},
	}

	podList, serviceList, secretList, configMapList, endPointsList, err := ParseMetaToV1List(results)
	if err != nil {
		return err
	}

	if len(podList.Items) != 0 {
		if len(podList.Items) != 1 {
			for _, info := range podList.Items {
				o := info.DeepCopyObject()
				list.Items = append(list.Items, runtime.RawExtension{Object: o})
			}

			listData, err := json.Marshal(list)
			if err != nil {
				return err
			}

			converted, err := runtime.Decode(unstructured.UnstructuredJSONScheme, listData)
			if err != nil {
				return err
			}
			obj = converted
		} else {
			obj = podList.Items[0].DeepCopyObject()
		}
		if err := PrintGeneric(printer, obj, out); err != nil {
			return err
		}
	}
	if len(serviceList.Items) != 0 {
		if len(serviceList.Items) != 1 {
			for _, info := range serviceList.Items {
				o := info.DeepCopyObject()
				list.Items = append(list.Items, runtime.RawExtension{Object: o})
			}

			listData, err := json.Marshal(list)
			if err != nil {
				return err
			}

			converted, err := runtime.Decode(unstructured.UnstructuredJSONScheme, listData)
			if err != nil {
				return err
			}
			obj = converted
		} else {
			obj = serviceList.Items[0].DeepCopyObject()
		}
		if err := PrintGeneric(printer, obj, out); err != nil {
			return err
		}
	}
	if len(secretList.Items) != 0 {
		if len(secretList.Items) != 1 {
			for _, info := range secretList.Items {
				o := info.DeepCopyObject()
				list.Items = append(list.Items, runtime.RawExtension{Object: o})
			}

			listData, err := json.Marshal(list)
			if err != nil {
				return err
			}

			converted, err := runtime.Decode(unstructured.UnstructuredJSONScheme, listData)
			if err != nil {
				return err
			}
			obj = converted
		} else {
			obj = secretList.Items[0].DeepCopyObject()
		}
		if err := PrintGeneric(printer, obj, out); err != nil {
			return err
		}
	}
	if len(configMapList.Items) != 0 {
		if len(configMapList.Items) != 1 {
			for _, info := range configMapList.Items {
				o := info.DeepCopyObject()
				list.Items = append(list.Items, runtime.RawExtension{Object: o})
			}

			listData, err := json.Marshal(list)
			if err != nil {
				return err
			}

			converted, err := runtime.Decode(unstructured.UnstructuredJSONScheme, listData)
			if err != nil {
				return err
			}
			obj = converted
		} else {
			obj = configMapList.Items[0].DeepCopyObject()
		}
		if err := PrintGeneric(printer, obj, out); err != nil {
			return err
		}
	}
	if len(endPointsList.Items) != 0 {
		if len(endPointsList.Items) != 1 {
			for _, info := range endPointsList.Items {
				o := info.DeepCopyObject()
				list.Items = append(list.Items, runtime.RawExtension{Object: o})
			}

			listData, err := json.Marshal(list)
			if err != nil {
				return err
			}

			converted, err := runtime.Decode(unstructured.UnstructuredJSONScheme, listData)
			if err != nil {
				return err
			}
			obj = converted
		} else {
			obj = endPointsList.Items[0].DeepCopyObject()
		}
		if err := PrintGeneric(printer, obj, out); err != nil {
			return err
		}
	}
	return nil
}

// HumanReadablePrint Output data in table form
func HumanReadablePrint(results []dao.Meta, printer printers.ResourcePrinter, out io.Writer) error {
	podList, serviceList, secretList, configMapList, endPointsList, err := ParseMetaToAPIList(results)
	if err != nil {
		return err
	}

	if len(podList.Items) != 0 {
		talbe, err := ConvertDataToTable(podList)
		if err != nil {
			return err
		}
		if err := printer.PrintObj(talbe, out); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(out); err != nil {
			return err
		}
	}
	if len(serviceList.Items) != 0 {
		talbe, err := ConvertDataToTable(serviceList)
		if err != nil {
			return err
		}
		if err := printer.PrintObj(talbe, out); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(out); err != nil {
			return err
		}
	}
	if len(secretList.Items) != 0 {
		talbe, err := ConvertDataToTable(secretList)
		if err != nil {
			return err
		}
		if err := printer.PrintObj(talbe, out); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(out); err != nil {
			return err
		}
	}
	if len(configMapList.Items) != 0 {
		talbe, err := ConvertDataToTable(configMapList)
		if err != nil {
			return err
		}
		if err := printer.PrintObj(talbe, out); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(out); err != nil {
			return err
		}
	}
	if len(endPointsList.Items) != 0 {
		talbe, err := ConvertDataToTable(endPointsList)
		if err != nil {
			return err
		}
		if err := printer.PrintObj(talbe, out); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(out); err != nil {
			return err
		}
	}
	return nil
}

// PrintGeneric Output object data to out stream through printer
func PrintGeneric(printer printers.ResourcePrinter, obj runtime.Object, out io.Writer) error {
	isList := meta.IsListType(obj)
	if isList {
		items, err := meta.ExtractList(obj)
		if err != nil {
			return err
		}

		// take the items and create a new list for display
		list := &unstructured.UnstructuredList{
			Object: map[string]interface{}{
				"kind":       "List",
				"apiVersion": "v1",
				"metadata":   map[string]interface{}{},
			},
		}
		if listMeta, err := meta.ListAccessor(obj); err == nil {
			list.Object["metadata"] = map[string]interface{}{
				"selfLink":        listMeta.GetSelfLink(),
				"resourceVersion": listMeta.GetResourceVersion(),
			}
		}

		for _, item := range items {
			list.Items = append(list.Items, *item.(*unstructured.Unstructured))
		}
		if err := printer.PrintObj(list, out); err != nil {
			return err
		}
	} else {
		var value map[string]interface{}
		data, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		if err := printer.PrintObj(&unstructured.Unstructured{Object: value}, out); err != nil {
			return err
		}
	}

	return nil
}

// ConvertDataToTable Convert the data into table kind to simulate the data sent by api-server
func ConvertDataToTable(obj runtime.Object) (runtime.Object, error) {
	to := metav1.TableOptions{}
	tc := storage.TableConvertor{TableGenerator: k8sprinters.NewTableGenerator().With(printersinternal.AddHandlers)}

	return tc.ConvertToTable(context.TODO(), obj, &to)
}

// ParseMetaToAPIList Convert the data to the corresponding list type according to the apiserver usage type
// Only use this type definition to get the table header processing handle,
// and automatically obtain the ColumnDefinitions of the table according to the type
// Only used by HumanReadablePrint.
func ParseMetaToAPIList(results []dao.Meta) (*api.PodList, *api.ServiceList, *api.SecretList, *api.ConfigMapList, *api.EndpointsList, error) {
	podList := &api.PodList{}
	serviceList := &api.ServiceList{}
	secretList := &api.SecretList{}
	configMapList := &api.ConfigMapList{}
	endPointsList := &api.EndpointsList{}
	value := make(map[string]interface{})

	for _, v := range results {
		switch v.Type {
		case ResourceTypePod:
			pod := api.Pod{}

			if err := json.Unmarshal([]byte(v.Value), &value); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			metadata, err := json.Marshal(value["metadata"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			spec, err := json.Marshal(value["spec"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			status, err := json.Marshal(value["status"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(metadata, &pod.ObjectMeta); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(spec, &pod.Spec); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(status, &pod.Status); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			pod.APIVersion = "v1"
			pod.Kind = v.Type
			podList.Items = append(podList.Items, pod)

		case ResourceTypeService:
			svc := api.Service{}
			if err := json.Unmarshal([]byte(v.Value), &value); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			metadata, err := json.Marshal(value["metadata"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			spec, err := json.Marshal(value["spec"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			status, err := json.Marshal(value["status"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(metadata, &svc.ObjectMeta); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(spec, &svc.Spec); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(status, &svc.Status); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			svc.APIVersion = "v1"
			svc.Kind = v.Type
			serviceList.Items = append(serviceList.Items, svc)
		case ResourceTypeSecret:
			secret := api.Secret{}
			if err := json.Unmarshal([]byte(v.Value), &value); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			metadata, err := json.Marshal(value["metadata"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			data, err := json.Marshal(value["data"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			typeTmp, err := json.Marshal(value["type"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(metadata, &secret.ObjectMeta); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(data, &secret.Data); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(typeTmp, &secret.Type); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			secret.APIVersion = "v1"
			secret.Kind = v.Type
			secretList.Items = append(secretList.Items, secret)
		case ResourceTypeConfigmap:
			cmp := api.ConfigMap{}
			if err := json.Unmarshal([]byte(v.Value), &value); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			metadata, err := json.Marshal(value["metadata"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			data, err := json.Marshal(value["data"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(metadata, &cmp.ObjectMeta); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(data, &cmp.Data); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			cmp.APIVersion = "v1"
			cmp.Kind = v.Type
			configMapList.Items = append(configMapList.Items, cmp)
		case ResourceTypeEndpoints:
			ep := api.Endpoints{}
			if err := json.Unmarshal([]byte(v.Value), &value); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			metadata, err := json.Marshal(value["metadata"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			subsets, err := json.Marshal(value["subsets"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(metadata, &ep.ObjectMeta); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(subsets, &ep.Subsets); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			ep.APIVersion = "v1"
			ep.Kind = v.Type
			endPointsList.Items = append(endPointsList.Items, ep)
		default:
			return nil, nil, nil, nil, nil, fmt.Errorf("Parsing failed, unrecognized type: %v. ", v.Type)
		}
	}

	return podList, serviceList, secretList, configMapList, endPointsList, nil
}

// ParseMetaToV1List Convert the data to the corresponding list type
// The type definition used by apiserver does not have the omitempty definition of json, will introduce a lot of useless null information
// Use v1 type definition to get data here
// Only used by JSONYamlPrint.
func ParseMetaToV1List(results []dao.Meta) (*v1.PodList, *v1.ServiceList, *v1.SecretList, *v1.ConfigMapList, *v1.EndpointsList, error) {
	podList := &v1.PodList{}
	serviceList := &v1.ServiceList{}
	secretList := &v1.SecretList{}
	configMapList := &v1.ConfigMapList{}
	endPointsList := &v1.EndpointsList{}
	value := make(map[string]interface{})

	for _, v := range results {
		switch v.Type {
		case ResourceTypePod:
			pod := v1.Pod{}

			if err := json.Unmarshal([]byte(v.Value), &value); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			metadata, err := json.Marshal(value["metadata"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			spec, err := json.Marshal(value["spec"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			status, err := json.Marshal(value["status"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(metadata, &pod.ObjectMeta); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(spec, &pod.Spec); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(status, &pod.Status); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			pod.APIVersion = "v1"
			pod.Kind = v.Type
			podList.Items = append(podList.Items, pod)

		case ResourceTypeService:
			svc := v1.Service{}
			if err := json.Unmarshal([]byte(v.Value), &value); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			metadata, err := json.Marshal(value["metadata"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			spec, err := json.Marshal(value["spec"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			status, err := json.Marshal(value["status"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(metadata, &svc.ObjectMeta); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(spec, &svc.Spec); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(status, &svc.Status); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			svc.APIVersion = "v1"
			svc.Kind = v.Type
			serviceList.Items = append(serviceList.Items, svc)
		case ResourceTypeSecret:
			secret := v1.Secret{}
			if err := json.Unmarshal([]byte(v.Value), &value); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			metadata, err := json.Marshal(value["metadata"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			data, err := json.Marshal(value["data"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			typeTmp, err := json.Marshal(value["type"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(metadata, &secret.ObjectMeta); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(data, &secret.Data); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(typeTmp, &secret.Type); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			secret.APIVersion = "v1"
			secret.Kind = v.Type
			secretList.Items = append(secretList.Items, secret)
		case ResourceTypeConfigmap:
			cmp := v1.ConfigMap{}
			if err := json.Unmarshal([]byte(v.Value), &value); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			metadata, err := json.Marshal(value["metadata"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			data, err := json.Marshal(value["data"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(metadata, &cmp.ObjectMeta); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(data, &cmp.Data); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			cmp.APIVersion = "v1"
			cmp.Kind = v.Type
			configMapList.Items = append(configMapList.Items, cmp)
		case ResourceTypeEndpoints:
			ep := v1.Endpoints{}
			if err := json.Unmarshal([]byte(v.Value), &value); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			metadata, err := json.Marshal(value["metadata"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			subsets, err := json.Marshal(value["subsets"])
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(metadata, &ep.ObjectMeta); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			if err := json.Unmarshal(subsets, &ep.Subsets); err != nil {
				return nil, nil, nil, nil, nil, err
			}
			ep.APIVersion = "v1"
			ep.Kind = v.Type
			endPointsList.Items = append(endPointsList.Items, ep)
		default:
			return nil, nil, nil, nil, nil, fmt.Errorf("Parsing failed, unrecognized type: %v. ", v.Type)
		}
	}

	return podList, serviceList, secretList, configMapList, endPointsList, nil
}

// IsFileExist check file is exist
func IsFileExist(path string) bool {
	_, err := os.Stat(path)

	return err == nil || !os.IsNotExist(err)
}

// IsAllowedFormat verification support format
func IsAllowedFormat(oFormat string) bool {
	for _, aFormat := range allowedFormats {
		if oFormat == aFormat {
			return true
		}
	}

	return false
}

// IsAvailableResources verification support resource type
func IsAvailableResources(rsT string) bool {
	_, ok := availableResources[rsT]
	return ok
}

// InitDB Init DB info
func InitDB(driverName, dbName, dataSource string) error {
	if err := orm.RegisterDriver(driverName, orm.DRSqlite); err != nil {
		return fmt.Errorf("Failed to register driver: %v ", err)
	}
	if err := orm.RegisterDataBase(
		dbName,
		driverName,
		dataSource); err != nil {
		return fmt.Errorf("Failed to register db: %v ", err)
	}
	orm.RegisterModel(new(dao.Meta))

	// create orm
	dbm.DBAccess = orm.NewOrm()
	if err := dbm.DBAccess.Using(dbName); err != nil {
		return fmt.Errorf("Using db access error %v ", err)
	}
	return nil
}

func (g *GetOptions) QueryDataFromDatabase(resType string, resNames []string) ([]dao.Meta, error) {
	var datas []dao.Meta
	fmt.Printf("type: %v, names: %v", resType, resNames)
	switch resType {
	case model.ResourceTypePod:
		pods, err := g.getPodsFromDatabase(g.Namespace, resNames)
		if err != nil {
			return nil, err
		}
		datas = append(datas, pods...)
	case model.ResourceTypeNode:
		node, err := g.getNodeFromDatabase(g.Namespace, resNames)
		if err != nil {
			return nil, err
		}
		datas = append(datas, node...)
	case model.ResourceTypeConfigmap, model.ResourceTypeSecret, constants.ResourceTypeEndpoints, constants.ResourceTypeService:
		value, err := g.getSingleResourceFromDatabase(g.Namespace, resNames, resType)
		if err != nil {
			return nil, err
		}
		datas = append(datas, value...)
	case ResourceTypeAll:
		pods, err := g.getPodsFromDatabase(g.Namespace, resNames)
		if err != nil {
			return nil, err
		}
		datas = append(datas, pods...)
		node, err := g.getNodeFromDatabase(g.Namespace, resNames)
		if err != nil {
			return nil, err
		}
		datas = append(datas, node...)

		resTypes := []string{model.ResourceTypeConfigmap, model.ResourceTypeSecret, constants.ResourceTypeEndpoints, constants.ResourceTypeService}
		for _, v := range resTypes {
			value, err := g.getSingleResourceFromDatabase(g.Namespace, resNames, v)
			if err != nil {
				return nil, err
			}
			datas = append(datas, value...)
		}
	default:
		return nil, fmt.Errorf("Query from database filed, type: %v,namespaces: %v. ", resType, g.Namespace)

	}

	return datas, nil
}

func (g *GetOptions) getPodsFromDatabase(resNS string, resNames []string) ([]dao.Meta, error) {
	var results []dao.Meta
	podJSON := make(map[string]interface{})
	podStatusJSON := make(map[string]interface{})

	podRecords, err := dao.QueryAllMeta("type", model.ResourceTypePod)
	if err != nil {
		return nil, err
	}
	for _, v := range *podRecords {
		namespaceParsed, _, _, _ := util.ParseResourceEdge(v.Key, model.QueryOperation)
		if namespaceParsed != resNS && !g.AllNamespace {
			continue
		}
		if len(resNames) > 0 && !IsExistName(resNames, v.Key) {
			continue
		}

		podKey := strings.Replace(v.Key, constants.ResourceSep+model.ResourceTypePod+constants.ResourceSep,
			constants.ResourceSep+model.ResourceTypePodStatus+constants.ResourceSep, 1)
		podStatusRecords, err := dao.QueryMeta("key", podKey)
		if err != nil {
			return nil, err
		}
		if len(*podStatusRecords) <= 0 {
			results = append(results, v)
			continue
		}
		if err := json.Unmarshal([]byte(v.Value), &podJSON); err != nil {
			return nil, err
		}
		podStatus, err := json.Marshal(*podStatusRecords)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(podStatus, &podStatusJSON); err != nil {
			return nil, err
		}
		podJSON["status"] = podStatusJSON["status"]
		data, err := json.Marshal(podJSON)
		if err != nil {
			return nil, err
		}
		v.Value = string(data)
		results = append(results, v)
	}

	return results, nil
}

func (g *GetOptions) getNodeFromDatabase(resNS string, resNames []string) ([]dao.Meta, error) {
	var results []dao.Meta
	nodeJSON := make(map[string]interface{})
	nodeStatusJSON := make(map[string]interface{})

	nodeRecords, err := dao.QueryAllMeta("type", model.ResourceTypeNode)
	if err != nil {
		return nil, err
	}
	for _, v := range *nodeRecords {
		namespaceParsed, _, _, _ := util.ParseResourceEdge(v.Key, model.QueryOperation)
		if namespaceParsed != resNS && !g.AllNamespace {
			continue
		}
		if len(resNames) > 0 && !IsExistName(resNames, v.Key) {
			continue
		}

		nodeKey := strings.Replace(v.Key, constants.ResourceSep+model.ResourceTypeNode+constants.ResourceSep,
			constants.ResourceSep+model.ResourceTypeNodeStatus+constants.ResourceSep, 1)
		nodeStatusRecords, err := dao.QueryMeta("key", nodeKey)
		if err != nil {
			return nil, err
		}
		if len(*nodeStatusRecords) <= 0 {
			results = append(results, v)
			continue
		}
		if err := json.Unmarshal([]byte(v.Value), &nodeJSON); err != nil {
			return nil, err
		}
		nodeStatus, err := json.Marshal(*nodeStatusRecords)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(nodeStatus, &nodeStatusJSON); err != nil {
			return nil, err
		}
		nodeJSON["status"] = nodeStatusJSON["status"]
		data, err := json.Marshal(nodeJSON)
		if err != nil {
			return nil, err
		}
		v.Value = string(data)
		results = append(results, v)
	}

	return results, nil
}

func (g *GetOptions) getSingleResourceFromDatabase(resNS string, resNames []string, resType string) ([]dao.Meta, error) {
	var results []dao.Meta

	resRecords, err := dao.QueryAllMeta("type", resType)
	if err != nil {
		return nil, err
	}
	for _, v := range *resRecords {
		namespaceParsed, _, _, _ := util.ParseResourceEdge(v.Key, model.QueryOperation)
		if namespaceParsed != resNS && !g.AllNamespace {
			continue
		}
		if len(resNames) > 0 && !IsExistName(resNames, v.Key) {
			continue
		}
		results = append(results, v)
	}

	return results, nil
}

func IsExistName(resNames []string, name string) bool {
	value := false
	for _, v := range resNames {
		if strings.Index(name, v) != -1 {
			value = true
		}
	}

	return value
}

// QueryMetaFromDatabase Filter data from the database based on conditions
func QueryMetaFromDatabase(isAllNamespace bool, resNamePaces string, resType string, resNames []string) ([]dao.Meta, error) {
	var results []dao.Meta

	if isAllNamespace {
		if resType == ResourceTypeAll || len(resNames) == 0 {
			results, err := dao.QueryMetaByRaw(
				fmt.Sprintf("select * from %v where %v.type in (%v)",
					dao.MetaTableName,
					dao.MetaTableName,
					availableResources[resType]))
			if err != nil {
				return nil, err
			}

			return results, nil
		}
		for _, resName := range resNames {
			result, err := dao.QueryMetaByRaw(
				fmt.Sprintf("select * from %v where %v.key like '%%/%v/%v'",
					dao.MetaTableName,
					dao.MetaTableName,
					strings.ReplaceAll(availableResources[resType], "'", ""),
					resName))
			if err != nil {
				return nil, err
			}
			results = append(results, result...)
		}

		return results, nil
	}
	if resType == ResourceTypeAll || len(resNames) == 0 {
		results, err := dao.QueryMetaByRaw(
			fmt.Sprintf("select * from %v where %v.key like '%v/%%' and  %v.type in (%v)",
				dao.MetaTableName,
				dao.MetaTableName,
				resNamePaces,
				dao.MetaTableName,
				availableResources[resType]))
		if err != nil {
			return nil, err
		}

		return results, nil
	}
	for _, resName := range resNames {
		result, err := dao.QueryMetaByRaw(
			fmt.Sprintf("select * from %v where %v.key = '%v/%v/%v'",
				dao.MetaTableName,
				dao.MetaTableName,
				resNamePaces,
				strings.ReplaceAll(availableResources[resType], "'", ""),
				resName))
		if err != nil {
			return nil, err
		}
		results = append(results, result...)
	}

	return results, nil
}

// FilterSelector Filter data that meets the selector
func FilterSelector(data []dao.Meta, selector string) ([]dao.Meta, error) {
	var results []dao.Meta
	var jsonValue = make(map[string]interface{})

	sLabels, err := SplitSelectorParameters(selector)
	if err != nil {
		return nil, err
	}
	for _, v := range data {
		err := json.Unmarshal([]byte(v.Value), &jsonValue)
		if err != nil {
			return nil, err
		}
		vLabel := jsonValue["metadata"].(map[string]interface{})["labels"]
		if vLabel == nil {
			results = append(results, v)
			continue
		}
		flag := true
		for _, sl := range sLabels {
			if !sl.Exist {
				flag = flag && vLabel.(map[string]interface{})[sl.Key] != sl.Value
				continue
			}
			flag = flag && (vLabel.(map[string]interface{})[sl.Key] == sl.Value)
		}
		if flag {
			results = append(results, v)
		}
	}

	return results, nil
}

// Selector filter structure
type Selector struct {
	Key   string
	Value string
	Exist bool
}

// SplitSelectorParameters Split selector args (flag: -l)
func SplitSelectorParameters(args string) ([]Selector, error) {
	var results = make([]Selector, 0)
	var sel Selector
	labels := strings.Split(args, ",")
	for _, label := range labels {
		if strings.Contains(label, "==") {
			labs := strings.Split(label, "==")
			if len(labs) != 2 {
				return nil, fmt.Errorf("Arguments in selector form may not have more than one \"==\". ")
			}
			sel.Key = labs[0]
			sel.Value = labs[1]
			sel.Exist = true
			results = append(results, sel)
			continue
		}
		if strings.Contains(label, "!=") {
			labs := strings.Split(label, "!=")
			if len(labs) != 2 {
				return nil, fmt.Errorf("Arguments in selector form may not have more than one \"!=\". ")
			}
			sel.Key = labs[0]
			sel.Value = labs[1]
			sel.Exist = false
			results = append(results, sel)
			continue
		}
		if strings.Contains(label, "=") {
			labs := strings.Split(label, "=")
			if len(labs) != 2 {
				return nil, fmt.Errorf("Arguments in selector may not have more than one \"=\". ")
			}
			sel.Key = labs[0]
			sel.Value = labs[1]
			sel.Exist = true
			results = append(results, sel)
		}
	}
	return results, nil
}
