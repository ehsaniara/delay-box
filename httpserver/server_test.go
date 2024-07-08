package httpserver

import (
	"reflect"
	"testing"
)

func Test_convertParameterToHeader(t *testing.T) {
	tests := []struct {
		name string
		args map[string]string
		want map[string][]byte
	}{
		{name: "positive test", args: map[string]string{"test": "dGVzdA=="}, want: map[string][]byte{"test": []byte("test")}},
		{name: "empty arg", args: make(map[string]string), want: make(map[string][]byte)},
		{name: "wrong base64 arg", args: map[string]string{"+++": "+++"}, want: make(map[string][]byte)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertParameterToHeader(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertParameterToHeader() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_convertHeaderToParameter(t *testing.T) {
	tests := []struct {
		name string
		args map[string][]byte
		want map[string]string
	}{
		{name: "positive test", args: map[string][]byte{"test": []byte("test")}, want: map[string]string{"test": "dGVzdA=="}},
		{name: "empty arg", args: make(map[string][]byte), want: make(map[string]string)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertHeaderToParameter(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertHeaderToParameter() = %v, want %v", got, tt.want)
			}
		})
	}
}
