package druid

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	"github.com/datainfrahq/druid-operator/controllers/druid/ext"
)

var deepStorageExtTypes = map[string]reflect.Type{}

func init() {
	deepStorageExtTypes["default"] = reflect.TypeOf(ext.DefaultDeepStorageManager{})
}

// We might have to add more methods to this interface to enable extensions that completely manage
// deploy, upgrade and termination of deep storage.
type deepStorageManager interface {
	Configuration() string
}

func createDeepStorageManager(spec *v1alpha1.DeepStorageSpec) (deepStorageManager, error) {
	if t, ok := deepStorageExtTypes[spec.Type]; ok {
		v := reflect.New(t).Interface()
		if err := json.Unmarshal(spec.Spec, v); err != nil {
			return nil, fmt.Errorf("Couldn't unmarshall deepStorage type[%s]. Error[%s].", spec.Type, err.Error())
		} else {
			return v.(deepStorageManager), nil
		}
	} else {
		return nil, fmt.Errorf("Can't find type[%s] for DeepStorage Mgmt.", spec.Type)
	}
}
