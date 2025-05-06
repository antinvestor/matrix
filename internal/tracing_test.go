package internal

import (
	"testing"

	"github.com/antinvestor/matrix/test/testrig"

	"github.com/stretchr/testify/assert"
)

func TestTracing(t *testing.T) {
	inctx, svc, cfg := testrig.Init(t, testOpts)
	defer svc.Stop(ctx)

	task, ctx := StartTask(inctx, "testing")
	assert.NotNil(t, ctx)
	assert.NotNil(t, task)
	assert.NotEqual(t, inctx, ctx)
	task.SetTag("key", "value")

	region, ctx2 := StartRegion(ctx, "testing")
	assert.NotNil(t, ctx)
	assert.NotNil(t, region)
	assert.NotEqual(t, ctx, ctx2)
	defer task.EndTask()
	defer region.EndRegion()
}
