package ingestion

import (
	"context"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	druidv1alpha1 "github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
)

// IngestionReconciler
type DruidIngestionReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
	// reconcile time duration, defaults to 10s
	ReconcileWait time.Duration
	Recorder      record.EventRecorder
}

func NewDruidIngestionReconciler(mgr ctrl.Manager) *DruidIngestionReconciler {
	return &DruidIngestionReconciler{
		Client:   mgr.GetClient(),
		Log:      ctrl.Log.WithName("controllers").WithName("Ingestion"),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("druid-ingestion"),
	}
}

// +kubebuilder:rbac:groups=druid.apache.org,resources=ingestions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=druid.apache.org,resources=ingestion/status,verbs=get;update;patch
func (r *DruidIngestionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logr := log.FromContext(ctx)

	druidIngestionCR := &v1alpha1.DruidIngestion{}
	err := r.Get(ctx, req.NamespacedName, druidIngestionCR)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if err := r.do(ctx, druidIngestionCR); err != nil {
		logr.Error(err, err.Error())
		return ctrl.Result{}, err
	} else {
		return ctrl.Result{RequeueAfter: LookupReconcileTime()}, nil
	}

}

func LookupReconcileTime() time.Duration {
	val, exists := os.LookupEnv("RECONCILE_WAIT")
	if !exists {
		return time.Second * 10
	} else {
		v, err := time.ParseDuration(val)
		if err != nil {
			// Exit Program if not valid
			os.Exit(1)
		}
		return v
	}
}

func (r *DruidIngestionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&druidv1alpha1.DruidIngestion{}).
		Complete(r)
}
