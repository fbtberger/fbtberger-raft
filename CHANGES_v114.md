# fbtberger-raft v114 — Der Append-Benchmark war falsch; er ist neu gebaut

## Die kaputten Zahlen
```
appendAndSync            100  wal   151418 µs ± 613270     ← Fehler 4× der Messwert
appendDeferringTheSync     1  bdb    36959 µs ± 251115     ← Fehler 7× der Messwert
appendDeferringTheSync     1  wal      331 µs ±   1146     ← Fehler 3,5×
```
Und der Widersinn, der es endgültig entlarvt: **BDB war mit „deferred sync" (37 ms) viermal
langsamer als mit vollem fsync (9,1 ms).** Ein Messwert, der die Physik verletzt, misst nicht die
Physik, sondern den Messfehler.

## Zwei Konstruktionsfehler
**1. Der Store wuchs während der Messung.** `@Setup(Level.Iteration)` legte ihn einmal pro Iteration
an — aber *innerhalb* der Iteration hängte jeder Aufruf weitere Einträge an. Bei ~100 µs/op und 1 s
Iteration sind das Zehntausende Aufrufe, bei `batchSize=100` also **Millionen Einträge**. Segmente
rollen, der B-Tree wächst, spätere Aufrufe kosten mehr als frühere. Gemessen wurde ein bewegliches
Ziel.

**2. `appendDeferringTheSync` war fire-and-forget.** Es stellte fsyncs schneller in die Warteschlange,
als die Platte sie abarbeiten konnte; der Rückstau wuchs, bis er kippte — daher die Ausreisser. Und
es verglich **zwei verschiedene Versprechen**: die eine Variante kehrte zurück, wenn die Daten
dauerhaft waren, die andere, wenn sie darum *gebeten* hatte.

## Der Neubau
- **Frischer Store pro Invocation** (`@Setup(Level.Invocation)`, wird **nicht** mitgemessen).
- **Feste Arbeitsmenge**: 200 Batches pro Invocation, `SingleShotTime`. Kein Driften.
- **Gleiche Dauerhaftigkeit bei beiden Varianten**: `appendDeferringTheSync` **wartet am Ende auf
  alle Futures** (`allOf(...).join()`). Damit bleibt als einziger Unterschied, ob das Verzögern die
  fsyncs **pipelinen** lässt — und genau das ist die Frage, die §10.2.1 stellt.
- **Neuer Parameter `prefill`** (0 / 10 000): Ein Append in einen leeren Store und einer in einen
  Store mit echtem Log dahinter sind verschiedene Operationen — und die zweite ist die, die ein
  laufender Cluster tatsächlich ausführt.

## Was gültig bleibt
Der **Recovery-Benchmark ist nicht betroffen** — er liest nur, aus einem Log fester Grösse. Seine
Kurven gelten:

```
WAL ≈  7 ms + 9,7 µs/Eintrag     (flacher Start, steile Steigung)
BDB ≈ 58 ms + 2,5 µs/Eintrag     (teurer Start, flache Steigung)
Kreuzung bei ≈ 7 000 Einträgen
```

Und ihre eigentliche Aussage steht ebenfalls: **beide Backends sind O(Log-Grösse) beim Start.** Die
Wahl verschiebt nur die Konstante. Das ist kein Backend-Problem, sondern ein **Snapshot-Problem**.

## Lauf
```
./gradlew jmh -Pjmh.args="StorageBenchmark.append -p impl=wal,bdb -p batchSize=1,100 -p prefill=0,10000"
```
Dauert länger als vorher (jeder Aufruf baut seinen Store neu auf) — dafür misst er etwas.

## Dateien
`StorageBenchmark.java`. Kein Produktionscode.
