# Controlled Mosquitto fixture

This fixture is local-only and contains no production credentials. Start it
with:

```sh
docker compose -f test/broker/compose.yaml up -d
```

The listeners provide these deterministic paths:

- `1883`: anonymous MQTT 3.1.1/5, retained messages and persistent broker
  state are available for session/reconciliation tests.
- `1884`: username/password authentication; invalid credentials exercise a
  rejected CONNACK without using the public broker. The deterministic `test` /
  `test` credential is available for authentication tests.
- `8080`: anonymous MQTT over WebSocket.
- `8883`: plaintext fixture listener reserved for adapter TLS configuration
  tests; it is not a TLS acceptance claim.
- `2883`: anonymous MQTT through the local delay proxy; broker-to-client
  packets are delayed by 250 ms to exercise delayed CONNACK/SUBACK/PUBACK
  completion without changing the broker or public service.
- `2884`: anonymous MQTT through an ACK-drop proxy; broker SUBACK packets are
  discarded to exercise missing-ACK timeout handling.
- `2885`: anonymous MQTT through a negative-ACK proxy; the first broker SUBACK
  reason code is replaced with MQTT 5 `0x87` (Not authorized) to exercise
  explicit rejection handling.
- `2886` and `2887`: negative-ACK proxies replacing the first PUBACK and
  UNSUBACK reason codes with MQTT 5 `0x87`, respectively.
- `2888`: a negative-ACK proxy replacing the first QoS 2 PUBREC reason code
  with MQTT 5 `0x87`.
- `2890`: a negative-ACK proxy replacing the first QoS 2 PUBCOMP reason code
- `2892`: a QoS 2 proxy delaying PUBCOMP by 500 ms after PUBREC
- `2893`: a QoS 2 proxy duplicating PUBCOMP
- `2894`: a QoS 2 proxy dropping PUBCOMP
- `2895`: a QoS 2 proxy disconnecting after PUBREC
  with MQTT 5 `0x92` (Packet Identifier Not Found).
- `2889`: a server-DISCONNECT proxy that sends MQTT 5 reason code `0x87`
  after CONNACK to exercise broker-initiated loss diagnostics.
- `2891`: closes the client-side TCP connection shortly after forwarding
  CONNECT, allowing a separate subscriber to observe an MQTT Will publish.

For CONNECT-property inspection, run `connect-inspect-proxy.mjs` separately
with `TARGET_HOST`, `TARGET_PORT`, `LISTEN_PORT`, and `REPORT_PATH`; it forwards
traffic and records only non-sensitive MQTT CONNECT metadata (including ordered
User Properties).

For scoped TLS adapter tests, `tls-fixture.conf` is mounted into a temporary
Mosquitto container together with generated CA/server/client material. It is
not part of the default compose fixture and no certificate or private key is
stored in the repository.

Network-down injection is performed by stopping this fixture container in the
test's own scope and restarting it afterward. The local `2884` proxy drops
SUBACK packets for a deterministic missing-ACK case, while `2883` provides
broker-to-client delay. The public Mosquitto service is never used for fault
injection or performance claims.

Stop and remove only this fixture with:

```sh
docker compose -f test/broker/compose.yaml down -v
```
