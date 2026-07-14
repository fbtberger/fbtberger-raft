# fbtberger-raft v101 — Replikation wird sichtbar (Diagnose, kein Verhaltenswechsel)

## Anlass
Auf dem dev-Cluster hingen **drei von fünf** Knoten (ein Voter + beide Learner) dauerhaft
zurück: Log bei Index 336 statt 340, State Machine **leer**, kein einziges Spiel angewendet.
Schreibvorgänge liefen weiter (Mehrheit 2/3 war intakt), Docker meldete alle Knoten `healthy`,
und in **keinem** Log stand ein Hinweis. Ein Client, der zufällig von einem dieser Knoten las,
bekam „Spiel nicht gefunden".

## Warum das unsichtbar war — zwei stumme Pfade
1. **`replicateTo()`**: fehlt für einen Peer der Transport, kehrt die Methode wortlos zurück.
   Ein konfiguriertes Mitglied wird dann von **niemandem** mehr repliziert — ohne eine Zeile Log.
2. **Der Fehlerpfad von `appendEntries()`**: `whenComplete((response, t) -> { if (t != null) { … } })`
   dekrementiert nur den Inflight-Zähler. Die Exception wird **verschluckt**. Ein Leader kann
   stundenlang erfolglos replizieren, ohne dass es irgendwo auftaucht.

## Was v101 ergänzt
- **`WARN` bei fehlendem Transport** (gedrosselt, 1×/5 s je Peer): „kein Transport vorhanden —
  dieser Peer wird NICHT repliziert".
- **`WARN` bei fehlgeschlagenem AppendEntries** (gedrosselt) inkl. `prevLogIndex`, Batch-Grösse
  und Ursache. Zusätzlich `metrics.replicationFailure()` — bisher wurde nur der *abgelehnte*,
  nicht der *fehlgeschlagene* Fall gezählt.
- **Replikations-Status des Leaders alle 10 s**, eine Zeile:
  ```
  replication: lastLogIndex=340 commitIndex=340 | kwatro1(voter) match=336 next=337 inflight=0 lastAck=182341ms LAGGING lastError=… | kwatro3(voter) match=340 … | kwatro4(learner) NO-TRANSPORT!
  ```
  `NO-TRANSPORT!` = Defekt (Mitglied ohne Transport), `LAGGING` = Rückstand, `lastAck` = wie lange
  der Peer schon nichts mehr bestätigt hat.

**Kein Verhaltenswechsel** — ausschliesslich Instrumentierung. Der eigentliche Fehler ist damit
noch nicht behoben; er ist damit **auffindbar**. Genau das war das Problem: ohne diese Zeilen ist
die Ursache (fehlender Transport vs. abgelehnte Einträge vs. Timeout) nicht zu unterscheiden.

## Dateien
`src/main/java/com/fbtberger/raft/RaftNode.java`.
