package nats

import "testing"

func TestCodecBytesPassthrough(t *testing.T) {
	b, _ := encode(JSONCodec{}, []byte("raw"))
	if string(b) != "raw" {
		t.Fatal(string(b))
	}
	got, _ := decode[[]byte](JSONCodec{}, []byte("raw"))
	if string(got) != "raw" {
		t.Fatal(string(got))
	}
}

func TestCodecJSON(t *testing.T) {
	type P struct {
		ID int `json:"id"`
	}
	b, _ := encode(JSONCodec{}, P{ID: 7})
	if string(b) != `{"id":7}` {
		t.Fatal(string(b))
	}
	p, err := decode[P](JSONCodec{}, b)
	if err != nil || p.ID != 7 {
		t.Fatal(p, err)
	}
	if _, err := decode[P](JSONCodec{}, []byte("{")); err == nil {
		t.Fatal("want decode error")
	}
}
