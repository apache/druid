package druidapi

import (
	"context"
	internalhttp "github.com/datainfrahq/druid-operator/pkg/http"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestGetAuthCreds(t *testing.T) {
	tests := []struct {
		name      string
		auth      Auth
		expected  internalhttp.BasicAuth
		expectErr bool
	}{
		{
			name: "default keys present",
			auth: Auth{
				Type:      BasicAuth,
				SecretRef: v1.SecretReference{Name: "test-default", Namespace: "test"},
			},
			expected:  internalhttp.BasicAuth{UserName: "test-user", Password: "test-password"},
			expectErr: false,
		},
		{
			name: "custom keys present",
			auth: Auth{
				Type:        BasicAuth,
				SecretRef:   v1.SecretReference{Name: "test", Namespace: "default"},
				UsernameKey: "usr",
				PasswordKey: "pwd",
			},
			expected:  internalhttp.BasicAuth{UserName: "admin", Password: "admin"},
			expectErr: false,
		},
		{
			name: "custom user key is  missing",
			auth: Auth{
				Type:        BasicAuth,
				SecretRef:   v1.SecretReference{Name: "test", Namespace: "default"},
				UsernameKey: "nope",
				PasswordKey: "pwd",
			},
			expected:  internalhttp.BasicAuth{},
			expectErr: true,
		},
		{
			name: "custom user key with default password key",
			auth: Auth{
				Type:        BasicAuth,
				SecretRef:   v1.SecretReference{Name: "test", Namespace: "default"},
				UsernameKey: "usr",
			},
			expected:  internalhttp.BasicAuth{UserName: "admin", Password: "also-admin"},
			expectErr: false,
		},
		{
			name: "custom password key is missing",
			auth: Auth{
				Type:        BasicAuth,
				SecretRef:   v1.SecretReference{Name: "test", Namespace: "default"},
				UsernameKey: "usr",
				PasswordKey: "nope",
			},
			expected:  internalhttp.BasicAuth{},
			expectErr: true,
		},
		{
			name:      "empty auth struct returns no creds",
			auth:      Auth{},
			expected:  internalhttp.BasicAuth{},
			expectErr: false,
		},
	}

	client := fake.NewClientBuilder().
		WithObjects(&v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-default",
				Namespace: "test",
			},
			Data: map[string][]byte{
				OperatorUserName: []byte("test-user"),
				OperatorPassword: []byte("test-password"),
			},
		}).
		WithObjects(&v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "default",
			},
			Data: map[string][]byte{
				"usr":            []byte("admin"),
				"pwd":            []byte("admin"),
				OperatorPassword: []byte("also-admin"),
			},
		}).Build()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := GetAuthCreds(context.TODO(), client, tt.auth)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestMakePath(t *testing.T) {
	tests := []struct {
		name            string
		baseURL         string
		componentType   string
		apiType         string
		additionalPaths []string
		expected        string
	}{
		{
			name:          "NoAdditionalPath",
			baseURL:       "http://example-druid-service",
			componentType: "indexer",
			apiType:       "task",
			expected:      "http://example-druid-service/druid/indexer/v1/task",
		},
		{
			name:            "OneAdditionalPath",
			baseURL:         "http://example-druid-service",
			componentType:   "indexer",
			apiType:         "task",
			additionalPaths: []string{"extra"},
			expected:        "http://example-druid-service/druid/indexer/v1/task/extra",
		},
		{
			name:            "MultipleAdditionalPaths",
			baseURL:         "http://example-druid-service",
			componentType:   "coordinator",
			apiType:         "rules",
			additionalPaths: []string{"wikipedia", "history"},
			expected:        "http://example-druid-service/druid/coordinator/v1/rules/wikipedia/history",
		},
		{
			name:          "EmptyBaseURL",
			baseURL:       "",
			componentType: "indexer",
			apiType:       "task",
			expected:      "druid/indexer/v1/task",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := MakePath(tt.baseURL, tt.componentType, tt.apiType, tt.additionalPaths...)
			if actual != tt.expected {
				t.Errorf("makePath() = %v, expected %v", actual, tt.expected)
			}
		})
	}
}
