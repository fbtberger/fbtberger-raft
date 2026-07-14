# fbtberger-raft v105 — Ehrlicher Leader-Kontakt, ehrliche Diagnose

Nachbereitung des Ausfalls (v104). Drei Dinge, die verhindert haben, dass der Cluster den
Zustand selbst gemeldet hat.

## 1. „Leader-Kontakt" hiess: Bytes angekommen
```java
currentLeaderId = request.getLeaderId();
lastLeaderContactMs = System.currentTimeMillis();   // ← ganz oben, vor jeder Verarbeitung
```
Ein Knoten, der bei **jedem** AppendEntries eine Exception warf (der `truncateFrom`-Bug), galt
damit als frisch kontaktiert und meldete gesunde Readiness — stundenlang, ohne einen einzigen
Eintrag anzuwenden.

**Neu:** Der Zeitstempel wird nur auf den Pfaden gesetzt, die auch **antworten** — der reguläre
Reject (der Leader spult dann zurück, §5.3) und der erfolgreiche Append. Fliegt eine Exception,
gibt es keinen Kontakt. Damit fällt so ein Knoten von selbst auf `DOWN`.

## 2. Ein Follower konnte seinen Rückstand nicht beziffern
**Neu:** `leaderCommitSeen()` (höchster vom Leader gemeldeter Commit-Index) und
`isCaughtUp()` = *ready* **und** `appliedIndex() >= leaderCommitSeen()`.
Dazu `HealthCheck.serving()` — strenger als `readiness()`: beantwortet „darf dieser Knoten
**Reads** ausliefern?". Ein Knoten mit leerer State Machine sagt jetzt:
`behind: applied=0 leaderCommit=348`.

## 3. `lastError` überlebte den Fehler
Die Statuszeile zeigte für einen längst genesenen Peer weiter
`match=348 lastAck=49ms lastError=UNAVAILABLE` — eine Diagnose, die lügt, ist schlimmer als keine.
**Neu:** bei jedem erfolgreichen Ack wird der letzte Fehler gelöscht.

## Dateien
`RaftNode.java`, `HealthCheck.java`.
