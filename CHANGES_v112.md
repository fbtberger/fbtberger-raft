# fbtberger-raft v112 — JMH-Benchmarks für die Storage-Schicht

## Grundsatz
Ein Benchmark ist nur etwas wert, wenn er eine **Frage** beantwortet. Diese beantworten drei — und
jede davon ist eine Entscheidung, die dieses Projekt bereits getroffen hat, bisher aus dem Bauch.

## Frage 1: Rechtfertigt der verzögerte fsync (§10.2.1) seine Komplexität?
`appendEntriesDeferSync` gibt an den Aufrufer zurück — der den **Raft-Lock hält** —, **bevor** der
fsync durch ist; Replikation und Plattensync laufen parallel, `leaderDiskMatchIndex` zieht nach. Das
ist echte Maschinerie, und sie lohnt sich nur, wenn der fsync tatsächlich der teure Teil ist.

`appendAndSync` gegen `appendDeferringTheSync` ist diese Zahl.

Zu lesen als **Latenz auf dem kritischen Pfad**, nicht als nachhaltiger Durchsatz: die verzögerte
Variante wartet nicht auf die Platte, ihre Kosten sind das, was der Raft-Lock wirklich bezahlt. Der
fsync passiert trotzdem — nur nicht dort, wo der Leader darauf blockiert.

## Frage 2: Was kostet ein Neustart — und wie wächst das? ⚠️
**Die operativ wichtigste.** kwatro fährt mit **abgeschalteten Snapshots**: Log-Replay *ist* die
Persistenz. Das funktioniert und hat die angenehme Eigenschaft, dass alte State-Machine-Fehler beim
Redeploy ausheilen. Es heisst aber auch, dass der Log **unbegrenzt wächst** — und
`recoverFromAnExistingLog` ist die Kosten **jedes** Deploys, für immer, als Funktion davon, wie lange
der Cluster schon läuft.

Gemessen wird über 1 000 / 10 000 / 50 000 Einträge. Der Sinn ist nicht die absolute Zahl, sondern die
**Form der Kurve**: Sie sagt, *wann* Snapshots aufhören, optional zu sein. Eine Zahl schlägt ein
Bauchgefühl — und diese Kurve beisst erst, wenn sie beisst.

## Frage 3: WAL oder Berkeley DB?
kwatros Daten-Knoten fahren `BerkeleyDbStorage`. `WalStorage` ist ein segmentierter Write-Ahead-Log
mit im Prinzip deutlich weniger Arbeit pro Append. **Alle** Benchmarks sind über beide (und
`InMemoryStorage` als Untergrenze — keine Platte) parametrisiert, damit die Wahl auf Belegen beruht.

Mitgemessen: `truncateAndReAppend` — der Follower-Aufholpfad, der im Juli den Cluster gekippt hat und
den ein gesunder Cluster nie betritt. Man sollte wissen, was er kostet, wenn er es doch tut.

## Bewusst KEIN Build-Gate
```
./gradlew jmh                                          # alles
./gradlew jmh -Pjmh.args="recoverFromAnExistingLog"    # nur einer
./gradlew jmh -Pjmh.args="-p impl=wal -p batchSize=10"
```
`jmh` hängt **nicht** an `build`. Benchmarks dauern Minuten und quälen die Platte; ein Build, der
scheitert, weil eine Messung auf einem ausgelasteten Laptop 8 % langsamer war, bringt Leuten bei,
den Build zu ignorieren. Sie sind ein **Messinstrument, kein Torwächter**.

## Kein neues Plugin
`jmh-core` und der Annotation-Processor standen bereits in `testImplementation` /
`testAnnotationProcessor` — die Benchmarks liegen in `src/test/java`, ein eigener Source-Set ist
unnötig. Der `jmh`-Task ist ein schlichter `JavaExec` auf `org.openjdk.jmh.Main`.

JUnit ignoriert die Klasse (keine `@Test`-Methoden), sie verlängert also den normalen Testlauf nicht.

## Dateien
Neu: `StorageBenchmark.java`. Geändert: `build.gradle.kts` (Task `jmh`). Kein Produktionscode.
