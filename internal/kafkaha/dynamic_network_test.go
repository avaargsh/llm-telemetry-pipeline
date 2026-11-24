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

func dockerNetworkHasContainer(raw, containerID string) (bool, error) {
    var containers map[string]json.RawMessage
    if err := json.Unmarshal([]byte(raw), &containers); err != nil {
        return false, fmt.Errorf("decode Docker network membership: %w", err)
    }
    _, ok := containers[containerID]
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

type dynamicEndpointReassignment struct {
    endpoints []dynamicBrokerEndpoint
}

func validateVoterIPSwap(before, after [2]string) error {
    for _, raw := range before {
        address, err := netip.ParseAddr(raw)
        if err != nil || !address.Is4() { return fmt.Errorf("invalid original voter IPv4: %q", raw) }
    }
    if before[0] == before[1] || after[0] != before[1] || after[1] != before[0] {
        return fmt.Errorf("expected cross-owner IPv4 swap: before=%v after=%v", before, after)
    }
    return nil
}

func haReassignDynamicBrokerEndpoints(t *testing.T, ctx context.Context, endpoints []dynamicBrokerEndpoint) *dynamicEndpointReassignment {
    t.Helper()
    if len(endpoints) == 0 { return nil }
    if len(endpoints) != 2 { t.Fatalf("expected two dynamic voter endpoints, got %d", len(endpoints)) }
    network := endpoints[0].network
    before := [2]string{endpoints[0].ipv4, endpoints[1].ipv4}
    if err := validateVoterIPSwap(before, [2]string{before[1], before[0]}); err != nil { t.Fatal(err) }
    for _, endpoint := range endpoints {
        if endpoint.network != network || endpoint.containerID == "" {
            t.Fatalf("inconsistent captured Docker endpoint for %s", endpoint.service)
        }
    }
    // A stopped Docker container may already have been detached by the
    // engine. Disconnect it if the endpoint still exists.
    for _, endpoint := range endpoints {
        raw, inspectErr := haDockerCommand(ctx, "network", "inspect", "--format", "{{json .Containers}}", network)
        if inspectErr != nil {
            t.Fatalf("inspect network %s before detaching %s: %v", network, endpoint.service, inspectErr)
        }
        attached, parseErr := dockerNetworkHasContainer(raw, endpoint.containerID)
        if parseErr != nil {
            t.Fatalf("verify network membership for %s: %v", endpoint.service, parseErr)
        }
        if attached {
            haDocker(t, ctx, "network", "disconnect", "-f", network, endpoint.containerID)
        }
    }


    // Use the other stopped voter's address, not a fresh address. The
    // explicitly configured bridge subnet permits Docker --ip assignments.
    for i, endpoint := range endpoints {
        args := []string{"network", "connect", "--ip", before[1-i]}
        for _, alias := range endpoint.aliases { args = append(args, "--alias", alias) }
        args = append(args, network, endpoint.containerID)
        haDocker(t, ctx, args...)
    }
    return &dynamicEndpointReassignment{endpoints: endpoints}
}

func (r *dynamicEndpointReassignment) verifyAndRelease(t *testing.T, ctx context.Context) map[string]map[string]string {
    t.Helper()
    if r == nil { return nil }
    before := [2]string{r.endpoints[0].ipv4, r.endpoints[1].ipv4}
    var after [2]string
    changes := make(map[string]map[string]string, 2)
    for i, endpoint := range r.endpoints {
        raw := haDocker(t, ctx, "inspect", "--format", "{{json .NetworkSettings.Networks}}", endpoint.containerID)
        networkName, newIP, aliases, err := parseDockerNetworkEndpoint(raw)
        if err != nil { t.Fatalf("inspect swapped %s endpoint after start: %v", endpoint.service, err) }
        if networkName != endpoint.network { t.Fatalf("unexpected Docker network for %s", endpoint.service) }
        found := false
        for _, alias := range aliases { if alias == endpoint.service { found = true; break } }
        if !found { t.Fatalf("Docker service alias %s was not restored", endpoint.service) }
        after[i] = newIP
        changes[endpoint.service] = map[string]string{"before_ipv4": endpoint.ipv4, "after_ipv4": newIP}
    }
    if err := validateVoterIPSwap(before, after); err != nil { t.Fatal(err) }
    t.Logf("verified cross-owner voter IPv4 swap: %v -> %v", before, after)
    return changes
}

func TestValidateVoterIPSwap(t *testing.T) {
    before := [2]string{"172.29.242.2", "172.29.242.3"}
    for _, tc := range []struct {name string; after [2]string; valid bool}{
        {"cross-owner swap", [2]string{"172.29.242.3", "172.29.242.2"}, true},
        {"fresh addresses", [2]string{"172.29.242.5", "172.29.242.6"}, false},
        {"unchanged", before, false},
        {"partial", [2]string{"172.29.242.3", "172.29.242.6"}, false},
        {"duplicate", [2]string{"172.29.242.3", "172.29.242.3"}, false},
    } {
        t.Run(tc.name, func(t *testing.T) {
            if got := validateVoterIPSwap(before, tc.after) == nil; got != tc.valid { t.Fatalf("valid=%v want %v", got, tc.valid) }
        })
    }
    for _, invalid := range [][2]string{{"invalid", "172.29.242.3"}, {"172.29.242.2", "172.29.242.2"}} {
        if validateVoterIPSwap(invalid, [2]string{invalid[1], invalid[0]}) == nil { t.Fatalf("accepted invalid original addresses: %v", invalid) }
    }
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

func TestDockerNetworkHasContainer(t *testing.T) {
    raw := "{\"container-a\":{\"Name\":\"broker2\"}}"
    attached, err := dockerNetworkHasContainer(raw, "container-a")
    if err != nil || !attached {
        t.Fatalf("expected container attached, got attached=%v err=%v", attached, err)
    }
    attached, err = dockerNetworkHasContainer(raw, "container-b")
    if err != nil || attached {
        t.Fatalf("expected container detached, got attached=%v err=%v", attached, err)
    }
}
