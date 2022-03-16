package medusa

import (
	"context"
	"testing"
	"time"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	ctrl "github.com/k8ssandra/k8ssandra-operator/controllers/k8ssandra"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	"github.com/k8ssandra/k8ssandra-operator/pkg/config"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	testutils "github.com/k8ssandra/k8ssandra-operator/pkg/test"
)

const (
	timeout  = time.Second * 5
	interval = time.Millisecond * 500
)

var (
	defaultStorageClass = "default"
	seedsResolver       = &fakeSeedsResolver{}
	managementApi       = &testutils.FakeManagementApiFactory{}
	medusaClientFactory *fakeMedusaClientFactory
)

func TestCassandraBackupRestore(t *testing.T) {
	ctx := testutils.TestSetup(t)
	ctx, cancel := context.WithCancel(ctx)
	testEnv1 := setupBackupTestEnv(t, ctx)
	defer testEnv1.Stop(t)
	t.Run("TestBackupDatacenter", testEnv1.ControllerTest(ctx, testBackupDatacenter))

	testEnv2 := setupRestoreTestEnv(t, ctx)
	defer testEnv2.Stop(t)
	t.Run("TestRestoreDatacenter", testEnv2.ControllerTest(ctx, testInPlaceRestore))

	testEnv3 := setupMedusaBackupTestEnv(t, ctx)
	defer testEnv3.Stop(t)
	t.Run("TestMedusaBackupDatacenter", testEnv3.ControllerTest(ctx, testMedusaBackupDatacenter))

	testEnv4 := setupMedusaTaskTestEnv(t, ctx)
	defer testEnv4.Stop(t)
	defer cancel()
	t.Run("TestMedusaTasks", testEnv4.ControllerTest(ctx, testMedusaTasks))
}

func setupBackupTestEnv(t *testing.T, ctx context.Context) *testutils.MultiClusterTestEnv {
	testEnv := &testutils.MultiClusterTestEnv{
		BeforeTest: func(t *testing.T) {
			managementApi.SetT(t)
			managementApi.UseDefaultAdapter()
		},
	}
	seedsResolver.callback = func(dc *cassdcapi.CassandraDatacenter) ([]string, error) {
		return []string{}, nil
	}

	reconcilerConfig := config.InitConfig()

	reconcilerConfig.DefaultDelay = 100 * time.Millisecond
	reconcilerConfig.LongDelay = 300 * time.Millisecond

	medusaClientFactory = NewMedusaClientFactory()

	err := testEnv.Start(ctx, t, func(mgr manager.Manager, clientCache *clientcache.ClientCache, clusters []cluster.Cluster) error {
		err := (&ctrl.K8ssandraClusterReconciler{
			ReconcilerConfig: reconcilerConfig,
			Client:           mgr.GetClient(),
			Scheme:           scheme.Scheme,
			ClientCache:      clientCache,
			ManagementApi:    managementApi,
		}).SetupWithManager(mgr, clusters)
		if err != nil {
			return err
		}
		err = (&CassandraBackupReconciler{
			ReconcilerConfig: reconcilerConfig,
			Client:           mgr.GetClient(),
			Scheme:           scheme.Scheme,
			ClientFactory:    medusaClientFactory,
		}).SetupWithManager(mgr)
		return err
	})
	if err != nil {
		t.Fatalf("failed to start test environment: %s", err)
	}
	return testEnv
}

func setupRestoreTestEnv(t *testing.T, ctx context.Context) *testutils.MultiClusterTestEnv {
	testEnv := &testutils.MultiClusterTestEnv{
		BeforeTest: func(t *testing.T) {
			managementApi.SetT(t)
			managementApi.UseDefaultAdapter()
		},
	}
	seedsResolver.callback = func(dc *cassdcapi.CassandraDatacenter) ([]string, error) {
		return []string{}, nil
	}

	reconcilerConfig := config.InitConfig()

	reconcilerConfig.DefaultDelay = 100 * time.Millisecond
	reconcilerConfig.LongDelay = 300 * time.Millisecond

	medusaClientFactory = NewMedusaClientFactory()

	err := testEnv.Start(ctx, t, func(mgr manager.Manager, clientCache *clientcache.ClientCache, clusters []cluster.Cluster) error {
		err := (&ctrl.K8ssandraClusterReconciler{
			ReconcilerConfig: reconcilerConfig,
			Client:           mgr.GetClient(),
			Scheme:           scheme.Scheme,
			ClientCache:      clientCache,
			ManagementApi:    managementApi,
		}).SetupWithManager(mgr, clusters)
		if err != nil {
			return err
		}
		err = (&MedusaTaskReconciler{
			ReconcilerConfig: reconcilerConfig,
			Client:           mgr.GetClient(),
			Scheme:           scheme.Scheme,
			ClientFactory:    medusaClientFactory,
		}).SetupWithManager(mgr)
		if err != nil {
			return err
		}

		err = (&CassandraRestoreReconciler{
			ReconcilerConfig: reconcilerConfig,
			Client:           mgr.GetClient(),
			Scheme:           scheme.Scheme,
			ClientFactory:    medusaClientFactory,
		}).SetupWithManager(mgr)
		return err
	})
	if err != nil {
		t.Fatalf("failed to start test environment: %s", err)
	}
	return testEnv
}

func setupMedusaBackupTestEnv(t *testing.T, ctx context.Context) *testutils.MultiClusterTestEnv {
	testEnv := &testutils.MultiClusterTestEnv{
		BeforeTest: func(t *testing.T) {
			managementApi.SetT(t)
			managementApi.UseDefaultAdapter()
		},
	}
	seedsResolver.callback = func(dc *cassdcapi.CassandraDatacenter) ([]string, error) {
		return []string{}, nil
	}

	reconcilerConfig := config.InitConfig()

	reconcilerConfig.DefaultDelay = 100 * time.Millisecond
	reconcilerConfig.LongDelay = 300 * time.Millisecond

	medusaClientFactory = NewMedusaClientFactory()

	err := testEnv.Start(ctx, t, func(mgr manager.Manager, clientCache *clientcache.ClientCache, clusters []cluster.Cluster) error {
		err := (&ctrl.K8ssandraClusterReconciler{
			ReconcilerConfig: reconcilerConfig,
			Client:           mgr.GetClient(),
			Scheme:           scheme.Scheme,
			ClientCache:      clientCache,
			ManagementApi:    managementApi,
		}).SetupWithManager(mgr, clusters)
		if err != nil {
			return err
		}
		err = (&MedusaTaskReconciler{
			ReconcilerConfig: reconcilerConfig,
			Client:           mgr.GetClient(),
			Scheme:           scheme.Scheme,
			ClientFactory:    medusaClientFactory,
		}).SetupWithManager(mgr)
		if err != nil {
			return err
		}
		err = (&MedusaBackupJobReconciler{
			ReconcilerConfig: reconcilerConfig,
			Client:           mgr.GetClient(),
			Scheme:           scheme.Scheme,
			ClientFactory:    medusaClientFactory,
		}).SetupWithManager(mgr)
		return err
	})
	if err != nil {
		t.Fatalf("failed to start test environment: %s", err)
	}
	return testEnv
}

//func setupMedusaRestoreTestEnv(t *testing.T, ctx context.Context) *testutils.MultiClusterTestEnv {
//	testEnv = &testutils.MultiClusterTestEnv{}
//	seedsResolver.callback = func(dc *cassdcapi.CassandraDatacenter) ([]string, error) {
//		return []string{}, nil
//	}
//
//	reconcilerConfig := config.InitConfig()
//
//	reconcilerConfig.DefaultDelay = 100 * time.Millisecond
//	reconcilerConfig.LongDelay = 300 * time.Millisecond
//
//	medusaClientFactory = NewMedusaClientFactory()
//
//	err := testEnv.Start(ctx, t, func(mgr manager.Manager, clientCache *clientcache.ClientCache, clusters []cluster.Cluster) error {
//		err := (&ctrl.K8ssandraClusterReconciler{
//			ReconcilerConfig: reconcilerConfig,
//			Client:           mgr.GetClient(),
//			Scheme:           scheme.Scheme,
//			ClientCache:      clientCache,
//			ManagementApi:    managementApi,
//		}).SetupWithManager(mgr, clusters)
//		if err != nil {
//			return err
//		}
//		err = (&MedusaTaskReconciler{
//			ReconcilerConfig: reconcilerConfig,
//			Client:           mgr.GetClient(),
//			Scheme:           scheme.Scheme,
//			ClientFactory:    medusaClientFactory,
//		}).SetupWithManager(mgr)
//		if err != nil {
//			return err
//		}
//
//		err = (&CassandraRestoreReconciler{
//			ReconcilerConfig: reconcilerConfig,
//			Client:           mgr.GetClient(),
//			Scheme:           scheme.Scheme,
//			ClientFactory:    medusaClientFactory,
//		}).SetupWithManager(mgr)
//		return err
//	})
//	if err != nil {
//		t.Fatalf("failed to start test environment: %s", err)
//	}
//	return testEnv
//}

func setupMedusaTaskTestEnv(t *testing.T, ctx context.Context) *testutils.MultiClusterTestEnv {
	testEnv := &testutils.MultiClusterTestEnv{
		BeforeTest: func(t *testing.T) {
			managementApi.SetT(t)
			managementApi.UseDefaultAdapter()
		},
	}
	seedsResolver.callback = func(dc *cassdcapi.CassandraDatacenter) ([]string, error) {
		return []string{}, nil
	}

	reconcilerConfig := config.InitConfig()

	reconcilerConfig.DefaultDelay = 100 * time.Millisecond
	reconcilerConfig.LongDelay = 300 * time.Millisecond

	medusaClientFactory = NewMedusaClientFactory()

	err := testEnv.Start(ctx, t, func(mgr manager.Manager, clientCache *clientcache.ClientCache, clusters []cluster.Cluster) error {
		err := (&ctrl.K8ssandraClusterReconciler{
			ReconcilerConfig: reconcilerConfig,
			Client:           mgr.GetClient(),
			Scheme:           scheme.Scheme,
			ClientCache:      clientCache,
			ManagementApi:    managementApi,
		}).SetupWithManager(mgr, clusters)
		if err != nil {
			return err
		}
		err = (&MedusaTaskReconciler{
			ReconcilerConfig: reconcilerConfig,
			Client:           mgr.GetClient(),
			Scheme:           scheme.Scheme,
			ClientFactory:    medusaClientFactory,
		}).SetupWithManager(mgr)
		if err != nil {
			return err
		}
		err = (&MedusaBackupJobReconciler{
			ReconcilerConfig: reconcilerConfig,
			Client:           mgr.GetClient(),
			Scheme:           scheme.Scheme,
			ClientFactory:    medusaClientFactory,
		}).SetupWithManager(mgr)
		return err
	})
	if err != nil {
		t.Fatalf("failed to start test environment: %s", err)
	}
	return testEnv
}

type fakeSeedsResolver struct {
	callback func(dc *cassdcapi.CassandraDatacenter) ([]string, error)
}

func (r *fakeSeedsResolver) ResolveSeedEndpoints(ctx context.Context, dc *cassdcapi.CassandraDatacenter, remoteClient client.Client) ([]string, error) {
	return r.callback(dc)
}
