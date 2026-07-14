# fbtberger-raft v109 — Die ReadIndex-Barriere wird jetzt tatsächlich ausgeführt

## Was fehlte
v108 hat den Quorums-Fehler behoben und mit `QuorumTest` die **Regel** bewiesen. Was fehlte, war der
Beweis, dass ein **laufender Knoten** sie auch anwendet — dieselbe Lücke wie bei v058 (Prädikat
getestet, Zustandsführung nicht).

Und die bestehenden ReadIndex-Tests in `RaftNodeTest` helfen dabei nicht: sie laufen alle gegen
einen **Ein-Knoten-Cluster**, wo `readIndex()` zurückkehrt, **bevor** die Barriere überhaupt gebaut
wird:

```java
if (majority() == 1) { return ...; }        // der getestete Pfad
ReadBarrier barrier = new ReadBarrier(ri);  // nie erreicht
```

Quorums-Bestätigung, die Voting-Member-Regel, das Verhalten bei Step-down: **nie ausgeführt.**
Abdeckung, die den Mechanismus umgeht, ist keine Abdeckung.

## Der skriptbare Peer
`ScriptedPeer` gewährt immer Stimmen (der Knoten wird zuverlässig Leader), bestätigt AppendEntries
aber **erst auf Ansage**. Schweigen ist dabei ein `CompletableFuture`, das nicht fertig wird — kein
Fehler: ein stummer Peer muss den Leader **im Amt lassen** (er tritt nur bei höherem Term ab), damit
die Barriere wirklich auf ein Quorum wartet und nicht auf einen Fehler reagiert.

## Tests
1. **`aReadIndexIsNotServedUntilAQuorumConfirmsTheLeadership`** — ohne Bestätigung kein Read. Genau
   dafür gibt es §6.4: ein abgesetzter Leader darf nicht aus seinem eigenen veralteten Zustand
   antworten.
2. **`aLearnersAcknowledgementCannotConfirmLeadership`** — ein Learner repliziert, aber er **wählt
   nicht**. Zählte sein Ack, könnte ein von allen Votern verlassener Leader sich seine Führung von
   Nicht-Wählern bestätigen lassen. Die `isVotingMember`-Schranke war nie ausgeführt worden.
3. **`aLeaderThatStepsDownFailsTheReadsItHadNotYetServed`** — wartende Reads scheitern mit
   `NotLeaderException`. Sie hängen zu lassen wäre schlecht; sie zu **erfüllen** wäre ein Read auf
   die Autorität einer Führung, die es nicht mehr gibt.
4. **`duringAConfigurationChange_aMajorityOfTheNewConfigurationAloneDoesNotConfirmLeadership`** —
   **der v108-Fehler, end-to-end.** C_old = {n1,n2,n3}, C_new = {n1,n4,n5}, Leader = n1. Ein Ack von
   n4 allein galt früher als genug (C_new: 2/3 ✅, C_old: 1/3 ❌). Jetzt wird der Read erst
   ausgeliefert, wenn **beide** Mehrheiten dahinterstehen.

Der Trick beim Joint-Test: Der Joint-Eintrag wird **beim Anhängen** wirksam, kann aber nicht
committen, solange C_old schweigt — genau damit hält man den Knoten im Joint-Zustand fest.

## Dateien
Neu: `ReadIndexBarrierTest.java`. Kein Produktionscode geändert.
