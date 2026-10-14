package nats

import "encoding/json"

// Codec encodes outgoing payloads and decodes incoming ones.
// Payloads of type []byte bypass the codec in both directions.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONCodec is the default Codec, backed by encoding/json.
type JSONCodec struct{}

// Marshal implements Codec.
func (JSONCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal implements Codec.
func (JSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func encode(c Codec, v any) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	return c.Marshal(v)
}

func decode[T any](c Codec, data []byte) (T, error) {
	var v T
	if p, ok := any(&v).(*[]byte); ok {
		*p = data
		return v, nil
	}
	err := c.Unmarshal(data, &v)
	return v, err
}
