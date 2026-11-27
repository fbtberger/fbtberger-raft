#!/usr/bin/env bash
#
# Copyright 2026 fbtBerger Technology
# SPDX-License-Identifier: Apache-2.0
#
# Certificates for the Docker Compose cluster, into ./certs (not committed).
#
#   ./certs.sh          create the CA, one certificate per node and one for clients;
#                       does nothing if they exist
#   ./certs.sh renew    issue new node and client certificates from the existing CA.
#                       Running nodes pick them up within tls.reload.interval.seconds,
#                       without a restart.
#
# Node certificates carry CN=node<N>: the peer transport checks the sender id of every
# Raft message against the CN of the certificate the connection authenticated with.
# Keys are PKCS#8 PEM ("BEGIN PRIVATE KEY"), the only form the nodes read.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs"
DAYS_CA=3650
DAYS_LEAF=90
mkdir -p "$DIR"

issue() {
    local name="$1" sans="$2" tmp
    tmp="$(mktemp -d)"
    openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$tmp/key" 2>/dev/null
    openssl req -new -key "$tmp/key" -subj "/CN=$name" -out "$tmp/csr"
    printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE\n' "$sans" > "$tmp/ext"
    openssl x509 -req -in "$tmp/csr" -CA "$DIR/ca.crt" -CAkey "$DIR/ca.key" -CAcreateserial \
        -days "$DAYS_LEAF" -sha256 -extfile "$tmp/ext" -out "$tmp/crt" 2>/dev/null
    chmod 644 "$tmp/crt" "$tmp/key"
    # Key first, certificate last, each by rename: a node that polls in between sees the
    # old certificate with the new key, refuses that pair and keeps what it has.
    mv "$tmp/key" "$DIR/$name.key"
    mv "$tmp/crt" "$DIR/$name.crt"
    rm -rf "$tmp"
    echo "issued $name (valid $DAYS_LEAF days)"
}

issue_all() {
    for n in 1 2 3; do
        issue "node$n" "DNS:raft-$n,DNS:localhost,IP:127.0.0.1"
    done
    issue "client" "DNS:localhost"
}

case "${1:-create}" in
    create)
        if [[ -f "$DIR/ca.crt" ]]; then
            echo "certificates exist in $DIR; use '$0 renew' for new node certificates"
            exit 0
        fi
        openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out "$DIR/ca.key" 2>/dev/null
        openssl req -x509 -new -key "$DIR/ca.key" -sha256 -days "$DAYS_CA" \
            -subj "/CN=fbtberger-raft-compose-ca" \
            -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
            -out "$DIR/ca.crt"
        chmod 600 "$DIR/ca.key"
        echo "created CA (valid $DAYS_CA days)"
        issue_all
        ;;
    renew)
        [[ -f "$DIR/ca.key" ]] || { echo "no CA in $DIR; run '$0' first" >&2; exit 1; }
        issue_all
        ;;
    *)
        echo "usage: $0 [create|renew]" >&2
        exit 2
        ;;
esac
