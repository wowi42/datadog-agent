// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

// +build kubeapiserver

package leaderelection

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakecorev1 "k8s.io/client-go/kubernetes/typed/core/v1/fake"
	core "k8s.io/client-go/testing"
	rl "k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	leaderNamespace = "default"
	leaseName       = "datadog-leader-election"
)

type testSuite struct {
	suite.Suite
}

func (s *testSuite) TestError() {
	_, err := GetLeaderEngine()
	require.NotNil(s.T(), err)
}

func TestSuite(t *testing.T) {
	s := &testSuite{}
	suite.Run(t, s)
}

func createLockObject(holderIdentity string) (obj runtime.Object) {
	objectMeta := metav1.ObjectMeta{
		Namespace: leaderNamespace,
		Name:      leaseName,
		Annotations: map[string]string{
			rl.LeaderElectionRecordAnnotationKey: fmt.Sprintf(`{"holderIdentity":"%s"}`, holderIdentity),
		},
	}
	return &v1.ConfigMap{ObjectMeta: objectMeta}
}

func TestLeaderLeaseDurationExpiration(t *testing.T) {
	c := &fakecorev1.FakeCoreV1{Fake: &core.Fake{}}

	le := LeaderEngine{
		LeaseName:       leaseName,
		LeaderNamespace: leaderNamespace,
		LeaseDuration:   1 * time.Second,
		HolderIdentity:  "foo",
		coreClient:      c,
	}

	c.AddReactor("get", "configmaps", func(action core.Action) (bool, runtime.Object, error) {
		return true, createLockObject("foo"), nil
	})

	var err error
	le.leaderElector, err = le.newElection(le.LeaseName, le.LeaderNamespace, le.LeaseDuration)
	require.NoError(t, err)

	le.EnsureLeaderElectionRuns()

	assert.True(t, le.IsLeader())

	// For whatever reason (like the leader losing connection to API server), simulate
	// another agent becoming the leader
	c.PrependReactor("get", "configmaps", func(action core.Action) (bool, runtime.Object, error) {
		return true, createLockObject("bar"), nil
	})

	time.Sleep(2 * time.Second)

	assert.False(t, le.IsLeader())

	c.PrependReactor("get", "configmaps", func(action core.Action) (bool, runtime.Object, error) {
		return true, createLockObject("foo"), nil
	})

	time.Sleep(2 * time.Second)

	assert.True(t, le.IsLeader())
}
