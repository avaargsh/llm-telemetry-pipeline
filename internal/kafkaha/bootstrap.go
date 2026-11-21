package kafkaha

import (
    "fmt"
    "strings"

    "github.com/segmentio/kafka-go"
)

// LiveISREndpoints produces a bounded, test-only host bootstrap list from
// freshly observed Kafka partition metadata. The current leader is first;
// only validated live ISR members are eligible. A stopped former leader must
// not remain in the bootstrap list after explicit client rebootstrap.
//
// hostPorts is the immutable Docker test mapping broker ID 1..N -> host port.
// These are NOT Kafka advertised network locations for production clients.
func LiveISREndpoints(p kafka.Partition, hostPorts []string, minISR int) ([]string, error) {
    if len(hostPorts) < 1 || len(hostPorts) > 9 {
        return nil, fmt.Errorf("invalid broker endpoint map size")
    }
    if err := RequireReplicaAndISR(p, len(hostPorts), minISR); err != nil {
        return nil, err
    }
    replica := map[int]struct{}{}
    for _, broker := range p.Replicas {
        if broker.ID < 1 || broker.ID > len(hostPorts) {
            return nil, fmt.Errorf("replica broker ID %d outside configured test endpoint map", broker.ID)
        }
        if _, duplicate := replica[broker.ID]; duplicate {
            return nil, fmt.Errorf("duplicate replica broker ID %d", broker.ID)
        }
        replica[broker.ID] = struct{}{}
    }
    isr := map[int]struct{}{}
    for _, broker := range p.Isr {
        if _, exists := replica[broker.ID]; !exists {
            return nil, fmt.Errorf("ISR broker ID %d is not a registered replica", broker.ID)
        }
        if _, duplicate := isr[broker.ID]; duplicate {
            return nil, fmt.Errorf("duplicate ISR broker ID %d", broker.ID)
        }
        isr[broker.ID] = struct{}{}
    }
    if _, ok := isr[p.Leader.ID]; !ok {
        return nil, fmt.Errorf("current leader %d is not in ISR", p.Leader.ID)
    }
    if len(isr) < minISR {
        return nil, fmt.Errorf("insufficient unique live ISR members: %d < %d", len(isr), minISR)
    }
    seeds := make([]string, 0, len(isr))
    appendBroker := func(id int) error {
        endpoint := strings.TrimSpace(hostPorts[id-1])
        if endpoint == "" {
            return fmt.Errorf("empty endpoint for live broker ID %d", id)
        }
        seeds = append(seeds, endpoint)
        return nil
    }
    if err := appendBroker(p.Leader.ID); err != nil {
        return nil, err
    }
    for _, broker := range p.Isr {
        if broker.ID == p.Leader.ID {
            continue
        }
        if err := appendBroker(broker.ID); err != nil {
            return nil, err
        }
    }
    return seeds, nil
}
