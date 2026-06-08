package exec

import "testing"

func TestLocalNestRecoveryWindowEscalatesRepeatedInactiveFailures(t *testing.T) {
	if got := localNestRecoveryWindow(1, 0, true); got != localNestInactiveRecovery {
		t.Fatalf("first inactive failure wait = %s, want %s", got, localNestInactiveRecovery)
	}

	if got := localNestRecoveryWindow(3, 0, true); got != localNestRecoveryMedium {
		t.Fatalf("repeated inactive failure wait = %s, want %s", got, localNestRecoveryMedium)
	}

	if got := localNestRecoveryWindow(6, 0, true); got != localNestRecoveryLong {
		t.Fatalf("many inactive failures wait = %s, want %s", got, localNestRecoveryLong)
	}

	if got := localNestRecoveryWindow(10, 0, true); got != localNestRecoveryMax {
		t.Fatalf("max inactive failures wait = %s, want %s", got, localNestRecoveryMax)
	}
}

func TestLocalNestRecoveryWindowEscalatesInactiveProbeFailures(t *testing.T) {
	if got := localNestRecoveryWindow(1, 2, true); got != localNestFlapRecoveryMin {
		t.Fatalf("inactive probe failures wait = %s, want %s", got, localNestFlapRecoveryMin)
	}
}
