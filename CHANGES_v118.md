# fbtberger-raft v118 — Gewipeter Peer holt per InstallSnapshot auf (stale matchIndex korrigiert)

## Der Fund (auf dev reproduziert, nach v117)
Ein Learner (kwatro-5) wurde einzeln gewiped (Volume gelöscht) und leer neu gestartet, während ein
Leader lief. v117 baute den toten Kanal korrekt neu auf — trotzdem blieb kwatro-5 dauerhaft auf
`appliedIndex=0`, der Leader eingefroren:

```
kwatro5(learner) match=730 next=731 inflight=1 lastAck=…  lastError=UNAVAILABLE
```

`match` klemmte auf **730** — dem Stand, den kwatro-5 **vor** dem Wipe bestätigt hatte —, obwohl der
Knoten nachweislich leer war. `nextIndex` fiel nie unter `snapshotIndex`, also schaltete
`replicateTo` **nie** auf InstallSnapshot um.

## Die Ursache
Der Reject-Pfad in `handleAppendEntriesResponse` setzte:

```java
long confirmed = matchIndex.getOrDefault(peerId, 0L);
nextIndex.put(peerId, Math.max(1, confirmed + 1));
```

`nextIndex = matchIndex + 1` ist nur korrekt, solange `matchIndex` **wahr** ist. Ein gewipeter Peer
verletzt das: sein Log ist auf 0 gekürzt, aber der Leader hält den alten, hohen `matchIndex`. Damit
kann `nextIndex` nie unter `matchIndex` (und nie unter `snapshotIndex`) fallen — der Peer wird ewig
an einem `prevLogIndex` geprüft, den er nie erfüllen kann, lehnt endlos ab und bleibt bei 0. Das ist
der Rest, der **v117 überlebt**: v117 heilt den *Kanal*, nicht den stale *Index* dahinter.

Klassisch ist dieser Zustand unmöglich (matchIndex ist monoton + dauerhaft auf dem Follower). Im
Split-Betrieb ist ein Einzel-Wipe/-Rebuild eines Datenknotens aber Alltag — und auf verteilten
Hosts (jeder frisch gebaute Knoten startet leer) wäre es der Normalfall, der **jeden** neuen Host
stranden ließe.

## Der Fix
Der Follower meldet beim Reject seinen eigenen `lastLogIndex`; der Leader zieht **beide** Zeiger
darauf herunter — monoton, denn ein Reject kann nur je *weniger* offenbaren, nie mehr.

- **Proto:** `AppendEntriesResponse.conflictLastLogIndex` (Feld 3). Auf Erfolg 0/ungesetzt.
- **Follower** (`handleAppendEntries`): beide Reject-Rückgaben setzen `conflictLastLogIndex =
  store.getLastLogIndex()`. Ein leerer Knoten meldet 0.
- **Leader** (`handleAppendEntriesResponse`, Reject-Zweig):
  ```java
  long hint = response.getConflictLastLogIndex();
  matchIndex.merge(peerId, hint, (cur, h) -> Math.min(cur, h));   // nie nach oben
  nextIndex.put(peerId, Math.max(1, Math.min(hint + 1, lastSentIndex)));
  ```
  Gewipeter Peer ⇒ `hint=0` ⇒ `matchIndex→0`, `nextIndex→1 ≤ snapshotIndex` ⇒ **InstallSnapshot in
  einer Runde**. Für einen normal-kürzeren Follower ist es zugleich ein O(1)-Backoff statt des alten
  Ein-Schritt-Dekrements.

Der **Transport-Fehler-Pfad** (v117) bleibt unangetastet: er trägt keine Log-Info, und sobald wieder
ein echter Reject fließt, korrigiert obiger Zweig den `matchIndex` — der Fehlerpfad leitet `nextIndex`
danach aus dem bereits korrigierten `matchIndex` ab.

## Verifikation
- `AppendEntriesRejectReportsLastLogIndexTest` — Follower-Seite, deterministisch: leerer Knoten
  lehnt mit `conflictLastLogIndex=0` ab; ein kürzerer Knoten meldet seinen echten `lastLogIndex`.
- `LeaderCatchesUpWipedPeerViaSnapshotTest` — Leader-Seite, End-to-End: n3 ackt bis zur Spitze
  (matchIndex hoch), Leader kompaktiert, n3 wird gewiped → muss per InstallSnapshot aufholen. Ohne
  Fix bleibt `nextIndex` über `snapshotIndex` gepinnt und n3 bekommt nie einen Snapshot (Timeout).

## Dateien
Geändert: `raft.proto` (`AppendEntriesResponse.conflictLastLogIndex`), `RaftNode`
(Follower-Reject × 2 melden `lastLogIndex`; Leader-Reject nutzt den Hint). Neu:
`AppendEntriesRejectReportsLastLogIndexTest`, `LeaderCatchesUpWipedPeerViaSnapshotTest`.
Proto-Change ⇒ Konsumenten (kwatro: beide Images) neu bauen.
