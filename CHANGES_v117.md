# fbtberger-raft v117 — Leader baut einen toten Peer-Kanal neu auf (statt ewig darauf zu retryen)

## Der Fund (auf dev reproduziert)
Ein Learner (kwatro-5) wurde einzeln neu erstellt, während **kwatro-1 Leader** war. Der Leader
zeigte danach dauerhaft:

```
kwatro5(learner) match=396 next=397 lastAck=758846ms
                 lastError=io.grpc.StatusRuntimeException: UNAVAILABLE: io exception
```

`match` eingefroren, `lastAck` über 12 min, kwatro-5 klebte bei `appliedIndex=0` — es bekam **kein
einziges AppendEntries**. DNS war korrekt (`kwatro-5` → aktuelle IP), der neue Container lief. Erst
ein **Neustart des Leaders** (→ kwatro-2 übernahm) löste es sofort: ein frischer Prozess baut frische
Kanäle. Und derselbe Einzel-Recreate lief unter dem neuen Leader problemlos.

Das ist mit hoher Wahrscheinlichkeit die Wurzel der Juli-Signatur **„Learner bei 0, Leader silent".**

## Die Ursache
`peerTransports` cached einen `RaftTransport` (gRPC `ManagedChannel`) pro Peer. Der Fehlerpfad in
`replicateTo` (`whenComplete`, `t != null`) setzt `nextIndex` zurück, loggt (v101) — **rührt den
Transport aber nie an.** Ein simpler `ManagedChannelBuilder.forTarget(...).usePlaintext()`-Kanal, der
gegen einen **neu erstellten** Container in einen anhaltenden io-Fehler gelaufen ist, löst sich nicht
zuverlässig selbst auf (kein Keepalive, keine erzwungene Neuauflösung) — der Leader hämmert denselben
toten Kanal endlos.

## Der Fix
Pro Peer die **aufeinanderfolgenden** Transport-Fehler zählen. Nach
`MAX_CONSECUTIVE_FAILURES_BEFORE_REBUILD` (3) in Folge den Transport **verwerfen und neu bauen**
(`rebuildTransportLocked`): alten Kanal `close()`, via `transportFactory.connect(address)` einen
frischen anlegen. Ein neuer Kanal macht frische DNS-Auflösung + frische Verbindung — genau das,
was den Peer entklemmt. Zähler wird bei **jedem Erfolg** und nach jedem Rebuild auf 0 gesetzt, so
dass ein frischer Kanal erst wieder 3 Fehler sammeln muss, bevor erneut gebaut wird (kein Thrashing).

Die Peer-Adresse kommt aus den Konfigurations-Maps (`currentConfiguration`/`currentLearners`/
`oldConfiguration`), mit `config.peerAddresses()` als Fallback. `peerTransports` ist eine
`ConcurrentHashMap` → das Remove/Put während der `sendHeartbeats`-Iteration ist sicher.

**Churn-Bremse.** Der häufige Fall ist *ein* toter Peer unter gesundem Leader — ohne Grenze würde
der Rebuild alle paar Heartbeats feuern, bis der Peer zurück ist. Ein Cooldown
(`REBUILD_COOLDOWN_MS = 2 s`) begrenzt das auf höchstens einen Rebuild alle 2 s pro Peer; der erste
Rebuild nach Ausfallbeginn feuert sofort (Zeitstempel-Default 0). Bei Erfolg werden Zähler und
Zeitstempel gelöscht.

## Test — rot ohne Fix, grün mit Fix
`LeaderRebuildsStuckPeerTransportTest`: Cluster {n1,n2,n3}, n1 unter Test, n2 gesunder Voter (liefert
die Mehrheit → n1 bleibt Leader und committet), **n3s erster Transport ist tot** (jede RPC
UNAVAILABLE). Über eine zählende `RaftTransportFactory`:

- **Ohne Fix:** `connect("n3")` wird genau **einmal** aufgerufen, der tote Kanal nie ersetzt →
  die Assertion `connectCount("n3") >= 2` läuft in den Timeout. Rot.
- **Mit Fix:** nach 3 Fehlern baut der Leader n3 neu (`connect("n3")` ein 2. Mal), und der frische,
  gesunde Transport bekommt wieder AppendEntries → beide Assertions grün.

Rot-Probe: im `whenComplete`-Fehlerzweig den `if (fails >= …) rebuildTransportLocked(peerId);`-Block
auskommentieren → die `connectCount >= 2`-Assertion schlägt fehl.

## ⚠️ ChaosTest angepasst (Modell, nicht Verhalten)
Der `ChaosTest` modellierte eine Partition als **Flag auf der Transport-Instanz**. Sobald der Leader
einen Transport neu baut (v117), bekam er eine frische, nicht-partitionierte Instanz → die Partition
heilte still, und `leaderPartitionedFromMajorityTriggersNewElection` bekam nie eine Neuwahl. Das war
eine zu fragile Annahme des Tests (Transporte seien permanent), kein Produktionsfehler: eine echte
Netzpartition heilt **nicht**, weil man einen neuen Kanal öffnet.

Fix im Test: der Partitions-/Loss-Zustand lebt jetzt **pro gerichteter Kante** (`LinkState`,
persistent über Rebuilds), nicht auf der wegwerfbaren Instanz. Ein neu gebauter Transport erbt den
Zustand seiner Kante; `partition`/`heal`/`setPacketLoss` mutieren die Kanten-Zustände. Semantik
unverändert, nur rebuild-fest. (`channels` ist jetzt threadsicher, da Rebuilds zur Laufzeit aus
mehreren Knoten-Threads Kanäle anlegen.)

## Betrieblicher Hinweis
Bis dieser Fix im Data-Image ist, gilt der bewährte Workaround: einen Peer **nicht** einzeln
recreaten, während ein Leader einen vergifteten Kanal zu ihm hält — oder, wenn es passiert, den
**Leader** kurz neu starten (kurze Neuwahl, Quorum bleibt). Mit v117 heilt der Leader den Kanal
nach ~3 Heartbeats selbst.

## Dateien
Geändert: `RaftNode` (`peerConsecutiveFailures` + Cooldown im Replikations-Fehlerpfad,
`rebuildTransportLocked`/`addressOf`), `ChaosTest` (Partition/Loss pro Kante, rebuild-fest). Neu:
`LeaderRebuildsStuckPeerTransportTest`. Kein Proto, keine öffentliche API, keine Storage-Änderung.
