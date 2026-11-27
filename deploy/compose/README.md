# Three-node cluster with Docker Compose

Three Raft nodes and a Keycloak, on one machine. Peers talk mutual TLS; clients
additionally need a Keycloak access token. Every node has a health check and a
Prometheus endpoint.

| Service | Ports on the host | What |
|---|---|---|
| `raft-1`, `raft-2`, `raft-3` | 9091, 9092, 9093 | Raft peers and the client channel (gRPC, mTLS) |
| | 10091, 10092, 10093 | `/metrics`, `/health`, `/ready` (HTTPS) |
| `keycloak` | 8080 | Realm `raft`, imported from `keycloak/raft-realm.json` |

## Start

From the repository root, build the jar the image is made of:

```
./gradlew shadowJar
```

Then, in this directory:

```
./certs.sh
docker compose up --build -d
docker compose ps
```

All four services report `healthy` after about a minute. If `quay.io` is not reachable,
`KEYCLOAK_IMAGE=keycloak/keycloak:26.0 docker compose up --build -d` takes the same
image from Docker Hub.

## Use

```
RAFT_KEYCLOAK_CLIENT_ID=raft-service RAFT_KEYCLOAK_CLIENT_SECRET=raft-service-dev-secret \
  java -cp ../../build/libs/fbtberger-raft-1.0.0-all.jar \
  com.fbtberger.raft.client.RaftClientDemo config/client.properties
```

`SET greeting hello`, `GET greeting`, `quit`. The client presents `certs/client.crt`
and fetches its token from Keycloak with the service account `raft-service`, which
holds the roles `raft-read` and `raft-write`.

Readiness and metrics of one node:

```
curl -k https://localhost:10091/ready
curl -k https://localhost:10091/metrics
```

## What is in the realm

| Who | Secret | Roles |
|---|---|---|
| client `raft-service` (service account) | `raft-service-dev-secret` | `raft-read`, `raft-write` |
| user `operator` (client `raft-cli`, password grant) | `operator` | `raft-read`, `raft-write`, `raft-admin` |
| user `reader` (client `raft-cli`, password grant) | `reader` | `raft-read` |

Keycloak admin console: http://localhost:8080, `admin` / `admin`.

These are development credentials and they are in this repository. Do not expose this
setup to a network you do not control; for anything real, point `auth.keycloak.issuer`
at your own Keycloak and issue certificates from your own CA.

## Renew certificates without a restart

```
./certs.sh renew
docker compose logs raft-1 | grep "TLS material reloaded"
```

Each node notices the new files within `tls.reload.interval.seconds` (10 here) and
uses them for every new connection. Nothing restarts and no connection is dropped.

## Try a failure

```
docker compose stop raft-1          # the other two elect a leader and keep serving
docker compose start raft-1         # it catches up from its volume and the leader
```

## Two data nodes and a witness

Uncomment `node.witness=true` in `config/node3.properties` before the first start
(or after `docker compose down -v`). Node 3 then votes and stores the log and holds
no state. See section 5.5 of `docs/architecture.md`.

## Stop

```
docker compose down          # keep the data volumes
docker compose down -v       # delete them
```

## Notes

- The nodes run with `-Draft.cli.enabled=false`: a container has no terminal, and
  without the switch the node would stop when its stdin ends.
- The node id in each `config/nodeN.properties` must equal the CN of that node's
  certificate. Under mTLS a peer's messages are checked against it.
- Tokens are issued under the name `http://localhost:8080` (`KC_HOSTNAME`) and the
  nodes fetch the realm keys from `http://keycloak:8080`. If clients reach Keycloak
  under another name, change `KC_HOSTNAME` and `auth.keycloak.issuer` together.
