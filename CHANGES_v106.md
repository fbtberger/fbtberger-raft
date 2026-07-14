# fbtberger-raft v106 — Eine Invarianten-Menge für alle Storage-Implementierungen

## Warum

v104 hat den Cursor-Bug in `BerkeleyDbStorage.truncateFrom()` behoben und den ersten Test für
Berkeley DB überhaupt mitgebracht. Das war die Symptombehandlung. Die eigentliche Ursache war die
**Testarchitektur**: drei Implementierungen desselben Interfaces, drei getrennte Testklassen, drei
auseinandergelaufene Invarianten-Mengen.

| Invariante | InMemory | BerkeleyDB | WAL |
|---|---|---|---|
| Snapshot-Grenze wandert nie rückwärts | ✅ | ❌ | ❌ |
| Truncate nach Snapshot | ❌ | ❌ | ✅ |
| Deferred Sync (§10.2.1) | ❌ | ❌ | ✅ |
| Truncate + Append (Follower-Aufholpfad) | ❌ | ✅ (erst v104) | ✅ |
| Truncate überlebt Reopen | ❌ | ✅ (erst v104) | ❌ |
| Log/Term/Vote überleben Reopen | — | ✅ (erst v104) | ✅ |

`InMemoryStorageTest` behauptete in seinem eigenen Javadoc, seine Invarianten gälten „equally to
BerkeleyDbStorage (same interface, same expected behaviour)". Genau in dieser falschen Annahme hat
der Truncate-Bug gelebt: die persistente Implementierung hat Verhalten — Transaktionen, Cursor —,
das die In-Memory-Variante gar nicht haben *kann*.

## Was sich ändert

**Neu: `RaftStorageContract`** — eine abstrakte Testklasse mit den Invarianten, die *jede*
`RaftStorage`-Implementierung erfüllen muss. Pro Implementierung eine Unterklasse mit einer Factory,
mehr nicht:

- `InMemoryStorageContractTest`
- `BerkeleyDbStorageContractTest` (echtes Berkeley-DB-Environment im Temp-Verzeichnis)
- `WalStorageContractTest`

**Durability:** `InMemoryStorage` persistiert per Design nichts. Die Reopen-Invarianten (Figure 2)
werden für sie über `Assumptions.assumeTrue` **übersprungen** statt stillschweigend als erfüllt
behauptet — das ist die ehrliche Fassung der Behauptung, die vorher im Javadoc stand.

**Neu im Contract**, über die Vereinigung der drei alten Klassen hinaus:
- `truncateHandlesALogLargerThanASingleCursorStep` — 200 Einträge; ein 3-Einträge-Log ist eine
  schwache Probe für eine Cursor-Schleife.
- `repeatedTruncateAndAppendCyclesConverge` — 5 Runden Truncate-alles/Append-alles: exakt die
  Schleife, in der der Leader nach dem Wurf `nextIndex` auf 1 zurücksetzte und alles erneut schickte.
- `deferredSyncEntriesAreImmediatelyReadableAndEventuallyDurable` — inkl. Reopen nach `future.get()`.
- `getTermAtATruncatedEntryIsMinusOne`, `truncateAfterASnapshotFallsBackToTheSnapshotBoundary`,
  `aSnapshotSurvivesAReopen`, `appendingPastASnapshotSurvivesAReopen` — jeweils jetzt für alle drei.

**Neu: `WalStorageSegmentTest`** — das, was wirklich WAL-spezifisch ist und der Contract nicht
erreichen kann: Segment-Rollover, Truncate über eine Segmentgrenze hinweg, Recovery über mehrere
Segmente, Löschen leergeräumter Segmente durch Kompaktierung.

## Entfernt

`InMemoryStorageTest`, `BerkeleyDbStorageTest`, `WalStorageTest` — vollständig absorbiert.
**`unzip -o` löscht nicht** → die drei Dateien müssen per `git rm` weg (Kommando in der Lieferung).

## Dateien

Neu: `src/test/java/com/fbtberger/raft/{RaftStorageContract,InMemoryStorageContractTest,
BerkeleyDbStorageContractTest,WalStorageContractTest,WalStorageSegmentTest}.java`
Geändert: `README.md` (Testbaum), `RaftNodeConfigurationTest.java` (Javadoc-Link zeigte auf eine
gelöschte Klasse).

## Erwartung an den Lauf

Die drei Contract-Klassen laufen dieselben ~30 Tests. Für `InMemoryStorage` werden die
Durability-Tests als *skipped* gemeldet — das ist beabsichtigt und soll so sichtbar bleiben.

Sollte eine der drei Implementierungen an einer Invariante scheitern, die sie bisher schlicht nie
gefordert bekam: **das ist der Zweck der Übung.** Bitte den Fehlschlag melden, nicht den Test
abschwächen.
