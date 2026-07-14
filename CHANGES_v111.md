# fbtberger-raft v111 — Die COW-Snapshot-Zusage ist jetzt eine geprüfte Zusage

## Befund: die Isolation ist korrekt — und ungeschützt
Die COW-Maschinerie in `RaftNode` ist **richtig**. `maybeTakeSnapshotLocked()` läuft unter dem
Raft-Lock, also werden das Etikett und der Zustand atomar gegenüber `apply()` genommen:

```java
long applied = lastApplied.get();                        // das Etikett
Supplier<byte[]> cow = stateMachine.prepareCowSnapshot(); // der Zustand
```

Sie hängt aber **vollständig** an einer Zusage, die jede State-Machine-Implementierung einhalten
muss — und die **nichts** geprüft hat.

## Die Falle
Diese Zeile kompiliert, liest sich richtig und ist schnell:

```java
public Supplier<byte[]> prepareCowSnapshot() { return this::takeSnapshot; }
```

Sie serialisiert **später**, auf dem Hintergrund-Thread — und sieht damit jeden `apply()`-Aufruf, der
inzwischen passiert ist. Der Snapshot trägt `lastIncludedIndex = N`, enthält aber N+1. Anschliessend
wird der Log bis N verworfen. **Beim Neustart wird N+1 ein zweites Mal angewendet.**

Bei idempotenten Kommandos passiert nichts Sichtbares — so überlebt so ein Fehler jahrelang. Bei
Kommandos, die Zustand **verbrauchen** (ein Zähler; ein Zug, der Karten aus einer Hand nimmt), ist es
stille Korruption.

Zweite Falle: eine **flache Kopie veränderlicher Werte**. `new HashMap<>(data)` isoliert die Map,
nicht ihren Inhalt. `KeyValueStateMachine` ist nur deshalb sicher, weil seine Werte `String` sind.

## ⚠️ Relevanz für kwatro
`KwatroStateMachine.games` hält **veränderliche `Game`-Objekte**. Eine flache Kopie würde dieselben
Instanzen teilen — der Hintergrund-Serialisierer sähe jeden Zug, der währenddessen gespielt wird.
Dass kwatro Snapshots **ausgeschaltet** hat (Log-Replay = Persistenz), ist derzeit das Einzige, was
davor schützt. Wer sie einschaltet, muss `prepareCowSnapshot()` überschreiben — tief kopieren oder
unter dem Lock serialisieren — und gegen den Contract fahren.

## Was geliefert wird
- **`StateMachineCowContract`** — die Invariante als ausführbare Zusicherungen: was nach der Erfassung
  angewendet wird, darf im Snapshot **nicht** auftauchen; die lebende State Machine läuft trotzdem
  weiter; zwei Erfassungen sind unabhängig.
- **`KeyValueStateMachineCowTest`** — der Contract gegen die vorhandene Implementierung.
- **`BrokenLazyStateMachineTest`** — die **Gegenprobe**. Eine absichtlich falsche State Machine (genau
  der Einzeiler oben) beweist, dass der Contract den Fehler **fängt**. Eine Contract-Suite, die nur
  gegen konforme Implementierungen läuft, beweist nichts über den Contract — sie beweist, dass die
  Implementierungen sich einig sind.
- **`StateMachine.prepareCowSnapshot()`**: das Javadoc benennt beide Fallen und verweist auf den
  Contract.

## Dateien
Neu: `StateMachineCowContract`, `KeyValueStateMachineCowTest`, `BrokenLazyStateMachineTest`.
Geändert: `StateMachine.java` (nur Javadoc). **Kein Verhaltenscode geändert.**
