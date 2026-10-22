package timeseries

import (
	"os"
	"testing"
)

// go-iotedge's GetDeviceDB relies on DBHandler handing out a fresh connection
// once the previous one was closed.
func TestDBHandlerReopensAfterClose(t *testing.T) {
	conf := GetDefaultDBConfig()
	conf.Name = "singleton_test.db"
	defer os.Remove(conf.Name)

	first := DBHandler(conf)
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	second := DBHandler(conf)
	defer second.Close()
	if err := second.DB.Ping(); err != nil {
		t.Fatalf("DBHandler after Close returned a closed handle: %v", err)
	}
}
