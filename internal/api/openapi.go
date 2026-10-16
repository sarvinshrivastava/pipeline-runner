package api

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

//go:embed openapi.yaml
var openapiYAML []byte

var (
	openapiOnce sync.Once
	openapiJSON []byte
	openapiErr  error
)

// OpenAPIJSON converts the embedded document once.
func OpenAPIJSON() ([]byte, error) {
	openapiOnce.Do(func() {
		var doc any
		if openapiErr = yaml.Unmarshal(openapiYAML, &doc); openapiErr != nil {
			return
		}
		openapiJSON, openapiErr = json.Marshal(doc)
	})
	return openapiJSON, openapiErr
}

func (s *Server) openapi(w http.ResponseWriter, _ *http.Request) {
	doc, err := OpenAPIJSON()
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(doc)
}

func newID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}
