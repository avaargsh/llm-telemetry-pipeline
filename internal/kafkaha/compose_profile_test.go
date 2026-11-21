package kafkaha

import "testing"

func TestHAComposeFileSelection(t *testing.T) {
 t.Setenv("M3B_HA_COMPOSE_FILE", "")
 if got:=haComposeFile();got!="deployments/docker-compose-ha.yml" {
   t.Fatalf("expected pinned lab profile by default, got %q",got)
 }
 t.Setenv("M3B_HA_COMPOSE_FILE","deployments/docker-compose-ha-dynamic.yml")
 if got:=haComposeFile();got!="deployments/docker-compose-ha-dynamic.yml" {
   t.Fatalf("dynamic-IP causal control ignored: %q",got)
 }
}
