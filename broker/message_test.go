package broker

import (
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"notifications/entity"
)

func TestEncodeDecode(t *testing.T) {
	n := entity.Notification{UserID: "user-1", Type: "email", Content: "hello"}

	m, err := Encode(n)
	require.NoError(t, err)

	assert.Equal(t, "user-1", string(m.Key))
	assert.Equal(t, "email", header(m, headerType))
	assert.JSONEq(t, `{"user_id":"user-1","content":"hello"}`, string(m.Value), "type передаётся в заголовке, а не в теле")

	got, err := Decode(m)
	require.NoError(t, err)
	assert.Equal(t, n, got)
}

func TestDecode_Broken(t *testing.T) {
	_, err := Decode(kafka.Message{Value: []byte("{not json")})
	assert.Error(t, err)

	_, err = Decode(kafka.Message{Value: []byte(`{"user_id":"u","content":"c"}`)})
	var ve *entity.ValidationError
	assert.ErrorAs(t, err, &ve)
}

func TestDeadLetter(t *testing.T) {
	m := kafka.Message{Topic: "user-notifications", Partition: 2, Offset: 7, Key: []byte("k"), Value: []byte("bad")}

	dl := DeadLetter(m, errors.New("decode payload"))

	assert.Equal(t, m.Key, dl.Key)
	assert.Equal(t, m.Value, dl.Value)
	assert.Equal(t, "decode payload", header(dl, headerError))
	assert.Equal(t, "user-notifications", header(dl, headerOriginalTopic))
	assert.Equal(t, "2", header(dl, headerOriginalPartition))
	assert.Equal(t, "7", header(dl, headerOriginalOffset))
}
