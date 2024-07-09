package httpserver

import (
	"reflect"
	"testing"
)

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
