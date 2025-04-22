package internal

import (
	"github.com/antinvestor/matrix/test/testrig"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTracing(t *testing.T) {
	inctx := testrig.NewContext(t)

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
