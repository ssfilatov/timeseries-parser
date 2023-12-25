package server

import (
	"bytes"
	"encoding/json"
	"github.com/golang/mock/gomock"
	"github.com/ssfilatov/ts/mocks"
	"github.com/ssfilatov/ts/pkg/partition"
	"github.com/ssfilatov/ts/pkg/record"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSelect(t *testing.T) {
	t.Run("Select", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m1 := mocks.NewMockPartition(ctrl)
		m2 := mocks.NewMockPartition(ctrl)

		m1.EXPECT().MaxTimestamp().Return(int64(50)).AnyTimes()
		m1.EXPECT().MinTimestamp().Return(int64(10)).AnyTimes()
		m2.EXPECT().MaxTimestamp().Return(int64(100)).AnyTimes()
		m2.EXPECT().MinTimestamp().Return(int64(60)).AnyTimes()

		m1.
			EXPECT().
			SelectRecords(gomock.Eq(int64(20)), gomock.Eq(int64(70))).
			Return([]*record.InternalRecord{{Timestamp: 50}}, nil)
		m2.
			EXPECT().
			SelectRecords(gomock.Eq(int64(20)), gomock.Eq(int64(70))).
			Return([]*record.InternalRecord{{Timestamp: 60}}, nil)
		buffer := bytes.Buffer{}
		buffer.Write([]byte("["))
		err := newHandler(nil).Select(&buffer,
			[]partition.Partition{m1, m2}, time.Unix(20, 0), time.Unix(70, 0))
		require.NoError(t, err)
		buffer.Write([]byte("]"))
		var records []*record.APIRecord
		require.NoError(t, json.Unmarshal(buffer.Bytes(), &records))
		require.Equal(t, []*record.APIRecord{
			{EventTime: time.Unix(50, 0).Format(time.RFC3339)},
			{EventTime: time.Unix(60, 0).Format(time.RFC3339)}},
			records)
	})

	t.Run("SelectLower", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m1 := mocks.NewMockPartition(ctrl)
		m2 := mocks.NewMockPartition(ctrl)

		m1.EXPECT().MaxTimestamp().Return(int64(50)).AnyTimes()
		m1.EXPECT().MinTimestamp().Return(int64(10)).AnyTimes()
		m2.EXPECT().MaxTimestamp().Return(int64(100)).AnyTimes()
		m2.EXPECT().MinTimestamp().Return(int64(60)).AnyTimes()

		buffer := bytes.Buffer{}
		buffer.Write([]byte("["))
		err := newHandler(nil).Select(&buffer,
			[]partition.Partition{m1, m2}, time.Unix(0, 0), time.Unix(5, 0))
		require.NoError(t, err)
		buffer.Write([]byte("]"))
		var records []*record.APIRecord
		require.NoError(t, json.Unmarshal(buffer.Bytes(), &records))
		require.Equal(t, []*record.APIRecord{}, records)
	})

	t.Run("SelectUpper", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m1 := mocks.NewMockPartition(ctrl)
		m2 := mocks.NewMockPartition(ctrl)

		m1.EXPECT().MaxTimestamp().Return(int64(50)).AnyTimes()
		m1.EXPECT().MinTimestamp().Return(int64(10)).AnyTimes()
		m2.EXPECT().MaxTimestamp().Return(int64(100)).AnyTimes()
		m2.EXPECT().MinTimestamp().Return(int64(60)).AnyTimes()

		buffer := bytes.Buffer{}
		buffer.Write([]byte("["))
		err := newHandler(nil).Select(&buffer,
			[]partition.Partition{m1, m2}, time.Unix(200, 0), time.Unix(300, 0))
		require.NoError(t, err)
		buffer.Write([]byte("]"))
		var records []*record.APIRecord
		require.NoError(t, json.Unmarshal(buffer.Bytes(), &records))
		require.Equal(t, []*record.APIRecord{}, records)
	})

	t.Run("Select", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m1 := mocks.NewMockPartition(ctrl)
		m2 := mocks.NewMockPartition(ctrl)

		m1.EXPECT().MaxTimestamp().Return(int64(50)).AnyTimes()
		m1.EXPECT().MinTimestamp().Return(int64(10)).AnyTimes()
		m2.EXPECT().MaxTimestamp().Return(int64(100)).AnyTimes()
		m2.EXPECT().MinTimestamp().Return(int64(60)).AnyTimes()

		m1.
			EXPECT().
			SelectRecords(gomock.Eq(int64(0)), gomock.Eq(int64(70))).
			Return([]*record.InternalRecord{{Timestamp: 50}}, nil)
		m2.
			EXPECT().
			SelectRecords(gomock.Eq(int64(0)), gomock.Eq(int64(70))).
			Return([]*record.InternalRecord{{Timestamp: 60}}, nil)
		buffer := bytes.Buffer{}
		buffer.Write([]byte("["))
		err := newHandler(nil).Select(&buffer,
			[]partition.Partition{m1, m2}, time.Unix(0, 0), time.Unix(70, 0))
		require.NoError(t, err)
		buffer.Write([]byte("]"))
		var records []*record.APIRecord
		require.NoError(t, json.Unmarshal(buffer.Bytes(), &records))
		require.Equal(t, []*record.APIRecord{
			{EventTime: time.Unix(50, 0).Format(time.RFC3339)},
			{EventTime: time.Unix(60, 0).Format(time.RFC3339)}},
		records)
	})
}
