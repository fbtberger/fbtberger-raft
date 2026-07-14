# fbtberger-raft v104 — Der eigentliche Fehler: `truncateFrom` committet bei offenem Cursor

## Die Ursache
```java
try (Cursor cursor = logDb.openCursor(txn, null)) {
    ... cursor.delete() ...
    txn.commit();          // ← INNERHALB des try-with-resources: Cursor noch offen
}
```
Berkeley DB verweigert das:
```
IllegalStateException: Transaction 128521 commit failed because there were open cursors.
    at BerkeleyDbStorage.truncateFrom(BerkeleyDbStorage.java:239)
```
**Jeder Truncate schlug fehl. Immer.** Der Fix ist die Reihenfolge: Cursor schliessen, *dann*
committen.

## Warum das monatelang unsichtbar war
`truncateFrom()` wird **nur** von einem Follower aufgerufen, der aufholen muss (AppendEntries
Regel 3). Ein Cluster, der nie einen Knoten verliert, betritt den Pfad nie. Auf dev sah es so aus:

1. kwatro-1/4/5 mussten nach einem Neustart aufholen → Leader schickt den Log ab Index 1.
2. Der Empfänger wirft in `truncateFrom` → gRPC antwortet mit `UNKNOWN`.
3. Der Leader setzt `nextIndex` auf 1 zurück, schickt **alle** Einträge erneut → Schleife.
4. **Drei von fünf Knoten** blieben stundenlang mit **leerer State Machine** stehen.
5. Der Cluster wirkte gesund: die verbliebenen zwei bildeten die Mehrheit, Schreibvorgänge liefen.
6. Clients, deren Lese-Anfrage auf einem leeren Knoten landete, bekamen „Spiel existiert nicht".

Drei stumme Fehlerpfade (v101/v103 behoben) haben verhindert, dass man das sieht: fehlender
Transport, verschlucktes AppendEntries-Ergebnis, nicht protokollierte Handler-Exception.

## Der fehlende Test — die eigentliche Lehre
Es gab **keinen einzigen Test für `BerkeleyDbStorage`**. `InMemoryStorageTest` behauptet in seinem
eigenen Javadoc:

> „Every invariant here applies equally to BerkeleyDbStorage (same interface, same expected
> behaviour) — these tests just run without needing a real Berkeley DB environment on disk."

Genau diese Annahme ist falsch: die persistente Implementierung hat Verhalten, das die
In-Memory-Variante gar nicht haben **kann** — Transaktionen und Cursor. Der neue
`BerkeleyDbStorageTest` läuft gegen ein echtes Environment im Temp-Verzeichnis und deckt ab:
- `truncateFrom` löscht ab Index (**die Regression** — schlug vorher fehl),
- Truncate + Append (die Sequenz eines aufholenden Followers),
- Idempotenz und Index jenseits des Logs,
- Persistenz von Log, Term und Vote über einen Reopen,
- **Truncate über einen Reopen** (war der Commit echt?),
- `saveSnapshotAndCompact` (Grenzterm bleibt bekannt, §7).

## Dateien
`src/main/java/com/fbtberger/raft/BerkeleyDbStorage.java` (Fix),
`src/test/java/com/fbtberger/raft/BerkeleyDbStorageTest.java` (neu).

## Nach dem Deploy
Die drei leeren Knoten holen von selbst auf — der Leader wiederholt ohnehin alle 5 s. In der
`replication:`-Zeile muss `match` bei allen Peers auf `lastLogIndex` steigen und `LAGGING`
verschwinden.
