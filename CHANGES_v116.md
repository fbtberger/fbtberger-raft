# fbtberger-raft v116 — Kein Snapshot während des Aufholens

## Der Fund
`maybeTakeSnapshotLocked` prüft die Schwelle **nach jedem** `applyCommittedEntries` — auch mitten
im häppchenweisen Aufholen eines Followers oder Learners. Beim Aufholen trägt ein AppendEntries-Batch
oft nur **einen Teil** dessen, was der Leader bereits committet hat; `commitIndex` wird deshalb auf
den letzten tatsächlich gespeicherten Eintrag gedeckelt:

```java
commitIndex.set(Math.min(request.getLeaderCommit(), lastNewIndex));
```

Der erste Batch, der die Schwelle überschreitet, konnte damit bei einem **Zwischenindex**
snapshotten und den Log bis dahin **kappen** — obwohl der Leader längst weiter ist und die restlichen
Einträge noch gar nicht angewendet sind. Ein Snapshot mit `lastIncludedIndex = M`, während der Knoten
in Wahrheit nur bis M von insgesamt N committeten Einträgen aufgeholt hat.

Real beobachtbar wurde das **nicht** im Betrieb (auf kwatro-5 fiel der Kanarienvogel-Snapshot exakt
bei 383, level mit dem Log). Aber es ist dieselbe Klasse von Fehler wie im Juli: eine
Zustandsentscheidung an einem Punkt treffen, an dem der Zustand noch gar nicht vollständig ist.

## Der Fix
Ein zusätzliches Gate in `maybeTakeSnapshotLocked`: snapshotten **nur, wenn level mit dem Leader**.

```java
if (appliedIndex() < leaderCommitSeen()) {
    return;   // noch am Aufholen — nicht kompaktieren
}
```

Das ist exakt das „bin ich wirklich aktuell?"-Prädikat aus `isCaughtUp()` (v105), **ohne die
Lease-Klausel**: ein hinkendes Lease darf einen Learner nicht am Kompaktieren hindern, ein hinkendes
*Apply* schon. Auf dem Leader kollabiert `leaderCommitSeen()` zu dessen `commitIndex`, auf den
`lastApplied` gerade eben hochgezogen wurde — dort also **transparent**.

### Die zweite Hälfte, ohne die das Gate blind wäre
`leaderCommitSeen` wurde bisher in `handleAppendEntries` **nach** `applyCommittedEntries` gesetzt. Der
Gate hätte während des partiellen Batches also noch den **gedeckelten** `commitIndex` gesehen
(`leaderCommitSeen()` = `max(leaderCommitSeen, commitIndex)`) und nichts geblockt. Deshalb wird die
Zeile jetzt **vor** den Apply-Block gezogen:

```java
leaderCommitSeen = Math.max(leaderCommitSeen, request.getLeaderCommit());   // v116: vor dem Apply
if (request.getLeaderCommit() > commitIndex.get()) {
    commitIndex.set(Math.min(request.getLeaderCommit(), lastNewIndex));
    applyCommittedEntries();
}
```

Die Zuweisung ist monoton (`max`) — sie ein paar Anweisungen früher zu machen, ändert nichts, was ein
Beobachter von außen sieht. Sie stellt nur sicher, dass das Gate den **wahren** Leader-Commit kennt,
bevor es entscheidet.

## Der Test — rot ohne Fix, grün mit Fix
`SnapshotCatchUpGateTest`: ein Follower bekommt Einträge 1..6 mit `leaderCommit=12` (Schwelle 5).
Nach Batch 1 ist `appliedIndex=6`, aber der Leader ist bei 12 → **kein Snapshot**. Batch 2 liefert
7..12, jetzt level → der aufgeschobene Snapshot fällt **genau einmal**.

Gemessen wird über einen zählenden `StateMachine`-Delegaten: `RaftNode` ruft
`prepareCowSnapshot()` **synchron unter dem Lock** auf, im selben Atemzug wie die Snapshot-
Entscheidung. Die eigentliche Kompaktierung läuft asynchron (COW, §5.1) und wäre als Assertion
zu wackelig — der COW-Aufruf ist das deterministische Signal.

- **Ohne Fix:** Batch 1 snapshottet sofort → `cowSnapshots()==1` → die erste Assertion (`==0`)
  schlägt fehl. Rot.
- **Mit Fix:** Batch 1 blockt (`==0`), Batch 2 löst genau einen aus (`==1`). Die zweite Assertion
  schützt zugleich gegen einen „Fix", der auf Followern einfach **nie** snapshottet.

## Dateien
Geändert: `RaftNode` (`maybeTakeSnapshotLocked`-Gate + `leaderCommitSeen` vor den Apply-Block).
Neu: `SnapshotCatchUpTest` → `SnapshotCatchUpGateTest.java`. Kein Änderung an Storage, Proto oder
öffentlicher API.
