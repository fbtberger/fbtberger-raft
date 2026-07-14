# fbtberger-raft v110 — Korrektur zu v109: das Harness verstiess gegen §4

`aLearnersAcknowledgementCannotConfirmLeadership` war rot: der Transport zum Learner entstand nie.

## Ursache — im Test, nicht in der Produktion
`startLeader()` wartete nur darauf, dass der Knoten **Leader wird**, und legte dann sofort n2 und n3
still. In diesem Moment ist der **No-op-Eintrag der neuen Amtszeit** zwar angehängt, aber noch nicht
committet — und kann es nun auch nie mehr werden, weil gerade die Mehrheit stummgeschaltet wurde.

Also griff:

```java
// §4 errata: a leader must commit an entry from its own term
// before accepting config changes, so it knows the latest committed configuration.
if (commitIndex.get() < leaderNoOpIndex) {
    return failedFuture(new ConfigurationChangeException(
        "leader has not yet committed an entry in its current term; retry shortly"));
}
```

`addLearner()` und `setConfiguration()` wurden **korrekt abgelehnt**. Die Implementierung hat sich an
den Algorithmus gehalten; mein Harness nicht.

## Fix
`startLeader()` wartet zusätzlich, bis der No-op **committet und angewendet** ist
(`appliedIndex() >= 1`). Die Peers bestätigen währenddessen — sie müssen, sonst kommt der No-op nie
durch. Erst danach nimmt der Test ihnen das Quorum weg.

Reihenfolge: Leader werden → No-op committen → stilllegen → Konfiguration ändern → Barriere prüfen.

Nur die Testdatei geändert.
