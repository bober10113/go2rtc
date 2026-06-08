package streams

import (
	"testing"
	"time"
)

func TestExecNestDerivedWarmupPolicyEscalatesAfterRecentFailures(t *testing.T) {
	timeout, stable, packets := execNestDerivedWarmupPolicy(0)
	if timeout != execNestDerivedWarmupTimeout || stable != execNestDerivedWarmupStable || packets != execNestDerivedWarmupMinPackets {
		t.Fatalf("base policy = (%s, %s, %d), want (%s, %s, %d)", timeout, stable, packets, execNestDerivedWarmupTimeout, execNestDerivedWarmupStable, execNestDerivedWarmupMinPackets)
	}

	timeout, stable, packets = execNestDerivedWarmupPolicy(2)
	if timeout != execNestDerivedFlapTimeout || stable != execNestDerivedFlapStable || packets != execNestDerivedFlapMinPackets {
		t.Fatalf("flap policy = (%s, %s, %d), want (%s, %s, %d)", timeout, stable, packets, execNestDerivedFlapTimeout, execNestDerivedFlapStable, execNestDerivedFlapMinPackets)
	}

	timeout, stable, packets = execNestDerivedWarmupPolicy(6)
	if timeout != execNestDerivedHardTimeout || stable != execNestDerivedHardStable || packets != execNestDerivedHardMinPackets {
		t.Fatalf("hard policy = (%s, %s, %d), want (%s, %s, %d)", timeout, stable, packets, execNestDerivedHardTimeout, execNestDerivedHardStable, execNestDerivedHardMinPackets)
	}
}

func TestRecentExecNestFailuresIgnoresStaleFailures(t *testing.T) {
	now := time.Now()
	prod := &Producer{
		execNestFailures: 4,
		execNestLastFail: now.Add(-execNestBackoffWindow -
			time.Second),
	}

	if got := prod.recentExecNestFailuresLocked(now); got != 0 {
		t.Fatalf("stale failures = %d, want 0", got)
	}

	prod.execNestLastFail = now.Add(-time.Minute)
	if got := prod.recentExecNestFailuresLocked(now); got != 4 {
		t.Fatalf("recent failures = %d, want 4", got)
	}
}
