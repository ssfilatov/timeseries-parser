package processor

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

func TestProcessRecords(t *testing.T) {
	tmpdir, err := os.MkdirTemp(os.TempDir(), "partitions")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	r := strings.NewReader(`2001-07-08T19:29:30Z dominique@schuster.com 2b457fa5-4453-475d-b9d1-f737e02ed732
2001-07-08T22:21:42Z rupert.halvorson@paucekstoltenberg.uk e1f358b9-079f-4c46-8fa4-4e9dc39c844f
2001-07-09T13:29:48Z rashawn.schmitt@bosco.com 47815165-5213-4001-8611-af20e8f1f5fc
`)
	partitions, err := NewProcessor(2, tmpdir).ProcessRecords(r, "sample1.txt")
	require.NoError(t, err)
	require.Len(t, partitions, 2)

	minTs1, err := time.Parse(time.RFC3339, "2001-07-08T19:29:30Z")
	require.NoError(t, err)
	maxTs1, err := time.Parse(time.RFC3339, "2001-07-08T22:21:42Z")
	require.NoError(t, err)
	require.Equal(t, minTs1.Unix(), partitions[0].MinTimestamp())
	require.Equal(t, maxTs1.Unix(), partitions[0].MaxTimestamp())


	minTs2, err := time.Parse(time.RFC3339, "2001-07-09T13:29:48Z")
	require.NoError(t, err)
	maxTs2, err := time.Parse(time.RFC3339, "2001-07-09T13:29:48Z")
	require.NoError(t, err)
	require.Equal(t, minTs2.Unix(), partitions[1].MinTimestamp())
	require.Equal(t, maxTs2.Unix(), partitions[1].MaxTimestamp())

	files, err := ioutil.ReadDir(tmpdir)
	require.NoError(t, err)
	require.Len(t, files, 4)
}
