package kafkaha

import (
    "context"
    "encoding/json"
    "fmt"
    "net/netip"
    "os/exec"
    "path/filepath"
    "strings"
    "testing"
    "time"
)

type dynamicBrokerEndpoint struct {
    service     string
    containerID string
    network     string
    ipv4        string
    aliases     []string
}

func parseDockerNetworkEndpoint(raw string) (string, string, []string, error) {
    var networks map[string]map[string]json.RawMessage
    if err := json.Unmarshal([]byte(raw), &networks); err != nil {
        return "", "", nil, fmt.Errorf("decode Docker network inspection: %w", err)
    }
    if len(networks) != 1 {
        return "", "", nil, fmt.Errorf("expected exactly one Docker network, got %d", len(networks))
    }
    for name, endpoint := range networks {
        if strings.TrimSpace(name) == "" {
            return "", "", nil, fmt.Errorf("empty Docker network name")
        }
        var rawIP string
        if err := json.Unmarshal(endpoint["IPAddress"], &rawIP); err != nil {
            return "", "", nil, fmt.Errorf("missing Docker IPv4 endpoint for network %q", name)
        }
        address, err := netip.ParseAddr(strings.TrimSpace(rawIP))
        if err != nil || !address.Is4() {
            return "", "", nil, fmt.Errorf("invalid Docker IPv4 endpoint for network %q", name)
        }
        var rawAliases []string
        if value, ok := endpoint["Aliases"]; ok {
            if err := json.Unmarshal(value, &rawAliases); err != nil {
                return "", "", nil, fmt.Errorf("invalid Docker aliases for network %q", name)
            }
        }
        aliases := make([]string, 0, len(rawAliases))
        seen := make(map[string]struct{}, len(rawAliases))
        for _, alias := range rawAliases {
            alias = strings.TrimSpace(alias)
            if alias == "" {
                continue
            }
            if _, exists := seen[alias]; exists {
                continue
            }
            seen[alias] = struct{}{}
            aliases = append(aliases, alias)
        }
        return name, address.String(), aliases, nil
    }
    return "", "", nil, fmt.Errorf("Docker network endpoint missing")
}

func dockerNetworkHasEndpoint(raw, network string) (bool, error) {
    var networks map[string]json.RawMessage
    if err := json.Unmarshal([]byte(raw), &networks); err != nil {
        return false, fmt.Errorf("decode Docker network inspection: %w", err)
    }
    _, ok := networks[network]
    return ok, nil
}

func dynamicNetworkProfile(composeFile string) bool {
    switch filepath.Base(composeFile) {
    case "docker-compose-ha-dynamic.yml", "docker-compose-ha-dns-ttl-zero.yml":
        return true
    default:
        return false
    }
}

func haDockerCommand(ctx context.Context, args ...string) (string, error) {
    cmd := exec.CommandContext(ctx, "docker", args...)
    cmd.Dir = "../.."
    output, err := cmd.CombinedOutput()
    if err != nil {
        return string(output), fmt.Errorf("docker %v: %w: %s", args, err, output)
    }
    return string(output), nil
}

func haDocker(t *testing.T, ctx context.Context, args ...string) string {
    t.Helper()
    output, err := haDockerCommand(ctx, args...)
    if err != nil {
        t.Fatal(err)
    }
    return output
}

func haCaptureDynamicBrokerEndpoints(t *testing.T, ctx context.Context, services []string) []dynamicBrokerEndpoint {
    t.Helper()
    if !dynamicNetworkProfile(haComposeFile()) {
        return nil
    }
    if len(services) != 2 {
        t.Fatalf("dynamic endpoint churn requires exactly two stopped voters, got %d", len(services))
    }
    endpoints := make([]dynamicBrokerEndpoint, 0, len(services))
    network := ""
    seenIPs := make(map[string]struct{}, len(services))
    for _, service := range services {
        containerID := strings.TrimSpace(haCompose(t, ctx, "ps", "-q", service))
        if containerID == "" {
            t.Fatalf("missing Docker container for %s", service)
        }
        raw := haDocker(t, ctx, "inspect", "--format", "{{json .NetworkSettings.Networks}}", containerID)
        networkName, ipv4, aliases, err := parseDockerNetworkEndpoint(raw)
        if err != nil {
            t.Fatalf("inspect %s Docker endpoint: %v", service, err)
        }
        if network == "" {
            network = networkName
        } else if networkName != network {
            t.Fatalf("dynamic voters are attached to different networks: %s vs %s", network, networkName)
        }
        if _, duplicate := seenIPs[ipv4]; duplicate {
            t.Fatalf("duplicate pre-fault Docker IPv4 %s", ipv4)
        }
        seenIPs[ipv4] = struct{}{}
        foundServiceAlias := false
        for _, alias := range aliases {
            if alias == service {
                foundServiceAlias = true
                break
            }
        }
        if !foundServiceAlias {
            aliases = append(aliases, service)
        }
        endpoints = append(endpoints, dynamicBrokerEndpoint{
            service: service, containerID: containerID, network: networkName,
            ipv4: ipv4, aliases: aliases,
        })
    }
    return endpoints
}

func haReassignDynamicBrokerEndpoints(t *testing.T, ctx context.Context, endpoints []dynamicBrokerEndpoint) map[string]map[string]string {
    t.Helper()
    if len(endpoints) == 0 {
        return nil
    }
    if len(endpoints) != 2 {
        t.Fatalf("expected two dynamic voter endpoints, got %d", len(endpoints))
    }
    network := endpoints[0].network
    oldIPs := make(map[string]struct{}, len(endpoints))
    for _, endpoint := range endpoints {
        if endpoint.network != network || endpoint.containerID == "" || endpoint.ipv4 == "" {
            t.Fatalf("inconsistent captured Docker endpoint for %s", endpoint.service)
        }
        if _, duplicate := oldIPs[endpoint.ipv4]; duplicate {
            t.Fatalf("duplicate captured IPv4 %s", endpoint.ipv4)
        }
        oldIPs[endpoint.ipv4] = struct{}{}
    }

    // A stopped Docker container may already have been detached by the
    // engine. Disconnect it if the endpoint still exists; in either case
    // verify the old endpoint is gone before reserving its IPv4.
    for _, endpoint := range endpoints {
        _, disconnectErr := haDockerCommand(ctx, "network", "disconnect", "-f", network, endpoint.containerID)
        raw, inspectErr := haDockerCommand(ctx, "inspect", "--format", "{{json .NetworkSettings.Networks}}", endpoint.containerID)
        if inspectErr != nil {
            t.Fatalf("inspect detached %s: %v", endpoint.service, inspectErr)
        }
        attached, parseErr := dockerNetworkHasEndpoint(raw, network)
        if parseErr != nil {
            t.Fatalf("verify detached %s: %v", endpoint.service, parseErr)
        }
        if attached {
            if disconnectErr != nil {
                t.Fatalf("disconnect %s from %s: %v", endpoint.service, network, disconnectErr)
            }
            t.Fatalf("%s still attached to %s after disconnect", endpoint.service, network)
        }
    }

    reservationNames := make([]string, 0, len(endpoints))
    cleanupReservations := func() {
        cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
        defer cancel()
        for _, name := range reservationNames {
            _, _ = haDockerCommand(cleanupCtx, "rm", "-f", name)
        }
    }
    defer cleanupReservations()

    // Occupy the released addresses with disposable non-Kafka containers so
    // Docker must allocate different IPv4 endpoints to the original brokers.
    for i, endpoint := range endpoints {
        name := fmt.Sprintf("m3b-ip-hold-%s-%d", endpoint.service, time.Now().UnixNano()+int64(i))
        reservationNames = append(reservationNames, name)
        haDocker(t, ctx, "run", "-d", "--rm", "--name", name,
            "--network", network, "--ip", endpoint.ipv4,
            "busybox:1.36", "sleep", "600")
    }

    // Reattach the same stopped broker containers, preserving service DNS
    // aliases and their named Kafka volumes, but let Docker choose new IPs.
    for _, endpoint := range endpoints {
        args := []string{"network", "connect"}
        for _, alias := range endpoint.aliases {
            args = append(args, "--alias", alias)
        }
        args = append(args, network, endpoint.containerID)
        haDocker(t, ctx, args...)
    }

    changes := make(map[string]map[string]string, len(endpoints))
    newIPs := make(map[string]struct{}, len(endpoints))
    for _, endpoint := range endpoints {
        raw := haDocker(t, ctx, "inspect", "--format", "{{json .NetworkSettings.Networks}}", endpoint.containerID)
        networkName, newIP, aliases, err := parseDockerNetworkEndpoint(raw)
        if err != nil {
            t.Fatalf("inspect reassigned %s endpoint: %v", endpoint.service, err)
        }
        if networkName != network || newIP == endpoint.ipv4 {
            t.Fatalf("Docker did not reassign %s: network=%s old=%s new=%s", endpoint.service, networkName, endpoint.ipv4, newIP)
        }
        if _, reused := oldIPs[newIP]; reused {
            t.Fatalf("Docker reused a reserved pre-fault IPv4 %s", newIP)
        }
        if _, duplicate := newIPs[newIP]; duplicate {
            t.Fatalf("Docker assigned duplicate post-fault IPv4 %s", newIP)
        }
        newIPs[newIP] = struct{}{}
        serviceAliasFound := false
        for _, alias := range aliases {
            if alias == endpoint.service {
                serviceAliasFound = true
                break
            }
        }
        if !serviceAliasFound {
            t.Fatalf("Docker service alias %s was not restored after reconnect", endpoint.service)
        }
        changes[endpoint.service] = map[string]string{"before_ipv4": endpoint.ipv4, "after_ipv4": newIP}
        t.Logf("forced Docker endpoint reassignment %s: %s -> %s", endpoint.service, endpoint.ipv4, newIP)
    }

    for _, name := range reservationNames {
        haDocker(t, ctx, "rm", "-f", name)
    }
    reservationNames = nil
    return changes
}

func TestParseDockerNetworkEndpoint(t *testing.T) {
    network, ip, aliases, err := parseDockerNetworkEndpoint("{\"telemetry_default\":{\"IPAddress\":\"172.18.0.3\",\"Aliases\":[\"broker2\",\"telemetry-broker2\",\"broker2\"]}}")
    if err != nil {
        t.Fatal(err)
    }
    if network != "telemetry_default" || ip != "172.18.0.3" {
        t.Fatalf("unexpected endpoint %q %q", network, ip)
    }
    if len(aliases) != 2 || aliases[0] != "broker2" || aliases[1] != "telemetry-broker2" {
        t.Fatalf("unexpected aliases %#v", aliases)
    }
}

func TestParseDockerNetworkEndpointRejectsAmbiguousOrInvalidInputs(t *testing.T) {
    cases := []string{
        "{}",
        "{\"one\":{\"IPAddress\":\"172.18.0.2\"},\"two\":{\"IPAddress\":\"172.18.0.3\"}}",
        "{\"one\":{\"IPAddress\":\"\"}}",
        "{\"one\":{\"IPAddress\":\"not-an-ip\"}}",
        "{\"one\":{\"IPAddress\":\"2001:db8::1\"}}",
    }
    for _, raw := range cases {
        if _, _, _, err := parseDockerNetworkEndpoint(raw); err == nil {
            t.Errorf("expected fail-closed rejection for %s", raw)
        }
    }
}

func TestDynamicNetworkProfile(t *testing.T) {
    if !dynamicNetworkProfile("deployments/docker-compose-ha-dynamic.yml") {
        t.Fatal("dynamic profile not recognized")
    }
    if !dynamicNetworkProfile("deployments/docker-compose-ha-dns-ttl-zero.yml") {
        t.Fatal("TTL-zero profile not recognized")
    }
    if dynamicNetworkProfile("deployments/docker-compose-ha.yml") {
        t.Fatal("pinned profile must not force endpoint churn")
    }
}

func TestDockerNetworkHasEndpoint(t *testing.T) {
    raw := "{\"network-a\":{\"IPAddress\":\"172.18.0.2\"}}"
    attached, err := dockerNetworkHasEndpoint(raw, "network-a")
    if err != nil || !attached {
        t.Fatalf("expected endpoint attached, got attached=%v err=%v", attached, err)
    }
    attached, err = dockerNetworkHasEndpoint(raw, "network-b")
    if err != nil || attached {
        t.Fatalf("expected endpoint detached, got attached=%v err=%v", attached, err)
    }
}
