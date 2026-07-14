# fbtberger-raft v115 — Default zurück auf BerkeleyDB; Migration wird symmetrisch

## Ich lag falsch, die Benchmarks haben es gezeigt
v113 hat den Default auf WAL gesetzt — **auf Intuition** („ein Append ist ein Anhängen, kein
B-Tree"), weil die Benchmarks noch nicht gelaufen waren. Sie sind jetzt gelaufen, auf der Maschine,
auf der der Cluster tatsächlich läuft:

| | WAL | BDB | |
|---|---|---|---|
| Recovery @ 50 000 | 378 ms | **49 ms** | BDB **7,8×** |
| Append (blockierender fsync) | 737 ms | **337 ms** | BDB 2,2× |
| Append (verzögerter fsync) | 356 ms | **303 ms** | BDB 1,2× |

**BDB gewinnt auf jeder gemessenen Achse.** Der WAL ist beim Recovery **linear** (er scannt jeden
Eintrag), BDB ist es nicht (er öffnet einen B-Tree). Der Default geht zurück — diesmal auf Belegen.

Das ist der Ertrag der Benchmarks: **sie haben dem widersprochen, der sie gebaut hat.** Genau dafür
gibt es sie.

(§10.2.1 ist trotzdem gerechtfertigt: der verzögerte fsync **halbiert** die Schreibkosten des WAL.
Er bringt BDB nur wenig, weil BDB seine Schreibvorgänge schon selbst gruppiert.)

## ⚠️ Die Falle, die v113 gestellt hat
`migrateBerkeleyDbToWalIfNeeded` fragte nur: *„gibt es schon einen WAL?"* Das reicht **genau einmal**
— auf dem Hinweg. Auf dem Rückweg ist es eine Falle:

Nachdem ein Knoten auf dem WAL gelaufen ist, sind die `.jdb`-Dateien auf den Stand der ersten
Migration **eingefroren**. Ein naives Zurückschalten auf `bdb` hätte diesen veralteten Log geladen —
**auf allen fünf Knoten gleichzeitig**. Keine Mehrheit hätte den aktuellen Stand gehalten, Raft
hätte nichts zum Heilen gehabt, und alles seit der Migration wäre weg gewesen. **Der Cluster hätte
dabei durchgehend `UP` gemeldet.**

## Der Fix: `StorageMigration.reconcile`
Symmetrisch, richtungsunabhängig. Beim Öffnen eines Backends wird verglichen, **was tatsächlich auf
der Platte liegt** — mit Rafts eigenem Aktualitätstest (§5.4.1: höherer letzter Term gewinnt; bei
Gleichstand der längere Log). Der weiter fortgeschrittene Log wird in das zu öffnende Backend
kopiert; der andere wird nach `superseded-<typ>-<zeitstempel>/` **verschoben, nie gelöscht**.

Idempotent — läuft bei jedem Start, kopiert aber nicht endlos hin und her.

## Neuer Benchmark: `replayTheWholeLogIntoAStateMachine`
Ich habe aus `recoverFromAnExistingLog` geschlossen, beide Backends seien O(Log-Grösse) beim Start
und unterschieden sich nur in der Konstanten. **Diese Zahl trug die Behauptung nicht:** Sie misst nur
das **Öffnen** des Storage. Ein startender Knoten liest zusätzlich **jeden Eintrag** zurück und wendet
ihn auf die State Machine an — und *das* ist O(n) bei **beiden** Backends.

Der neue Benchmark misst genau das. Er ist die Zahl, an der die Snapshot-Frage wirklich hängt, und er
hätte vor der Behauptung existieren müssen.

```
./gradlew jmh -Pjmh.args="StorageBenchmark.replay -p impl=wal,bdb"
```

## Dateien
`StorageMigration` (neu geschrieben: `reconcile`), `RaftStorageFactory` (Default `bdb`),
`RaftNodeConfiguration`, `StorageMigrationTest` (neu), `StorageBenchmark` (Replay-Benchmark).
