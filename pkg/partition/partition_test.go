package partition

import (
	"github.com/ssfilatov/ts/pkg/record"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSelectBinary(t *testing.T) {
	records := []*record.InternalRecord{
		{
			Timestamp: 10,
		},
		{
			Timestamp: 20,
		},
		{
			Timestamp: 30,
		},
		{
			Timestamp: 40,
		},
		{
			Timestamp: 50,
		},
	}

	require.Len(t, selectBinary(20, 50, records), 4)
	require.Len(t, selectBinary(20, 60, records), 4)
	require.Len(t, selectBinary(10, 60, records), 5)
	require.Len(t, selectBinary(0, 60, records), 5)
	require.Len(t, selectBinary(0, 10, records), 1)
	require.Len(t, selectBinary(0, 5, records), 0)
	require.Len(t, selectBinary(50, 70, records), 1)
	require.Len(t, selectBinary(60, 70, records), 0)
}
