package queue

import "fmt"

const (
	keyDispatchReady = "queue:dispatch_ready"
	keyInflight      = "inflight:jobs"
)

func tenantQueueKey(tenantID string) string {
	return fmt.Sprintf("queue:tenant:%s", tenantID)
}

func inflightCounterKey(tenantID string) string {
	return fmt.Sprintf("inflight:%s", tenantID)
}

func visibilityKey(jobID string) string {
	return fmt.Sprintf("vis:%s", jobID)
}
