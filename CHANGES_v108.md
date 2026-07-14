# fbtberger-raft v108 — Drei Antworten auf „habe ich ein Quorum?", zwei davon falsch

## Der Auftrag war „Tests für ReadIndex und Joint Consensus"
Beim Schreiben der Tests kam ein **Fehler** zum Vorschein. Der Reihe nach.

## Befund 1: ReadIndex war *scheinbar* getestet
`RaftNodeTest` hat `readIndexCompletesImmediatelyForSingleNode`, `readIndexFailsOnFollower` und
einen Test, dass der Index dem committeten entspricht. Alle laufen gegen einen **Ein-Knoten-Cluster**
— und genau dort überspringt `readIndex()` die Barriere vollständig:

```java
if (majority() == 1) { return ...; }          // ← der getestete Pfad
ReadBarrier barrier = new ReadBarrier(ri, majority());   // ← nie erreicht
```

`ReadBarrier`, `confirm`, `isVotingMember`, das Scheitern bei Step-down: **nie ausgeführt.** Dasselbe
Muster wie bei `BerkeleyDbStorage` — Tests, die wie Abdeckung aussehen, aber den Mechanismus umgehen.

## Befund 2: der eigentliche Fehler
Die Frage „habe ich ein Quorum?" wurde an **drei** Stellen beantwortet, und **zwei waren falsch**:

| Stelle | Regel | korrekt? |
|---|---|---|
| `advanceCommitIndex` | getrennte Mehrheiten in C_old **und** C_new | ✅ |
| **ReadIndex-Barriere** | `confirmed.size() + 1 >= max(\|Cold\|/2+1, \|Cnew\|/2+1)` | ❌ |
| **`hasValidLease()`** | zählt **nur** über `currentConfiguration` | ❌ |

Joint Consensus (§4.3) fragt nicht „wie viele haben bestätigt?", sondern „hat eine Mehrheit von
C_old bestätigt **und** eine Mehrheit von C_new?". Eine Zählung über der Vereinigung kann die zweite
Frage nicht ausdrücken.

**Gegenbeispiel** — C_old = {a,b,c}, C_new = {a,d,e}, Leader = a:

```
alte Regel:      max(2, 2) = 2   ->  {a, d} galt als genug
C_new-Mehrheit:  {a, d} = 2/3    ->  erfüllt
C_old-Mehrheit:  {a}    = 1/3    ->  NICHT erfüllt
```

Der Leader hätte seine Führung bestätigt — und einen angeblich **linearisierbaren Read** ausgeliefert
sowie eine gesunde Lease gemeldet (`isReadyToServe()` → `/health` = `UP`) — **ohne jede Mehrheit in
C_old hinter sich.** Während einer Mitgliedschaftsänderung ist das exakt die Garantie, für die es
das ReadIndex-Protokoll überhaupt gibt.

**Warum nie etwas passiert ist:** der Cluster war nie in Joint Consensus. Derselbe Grund, aus dem
`truncateFrom` monatelang überlebt hat — ein Pfad, den man nie betritt, ist ein Pfad, in dem ein
Fehler wohnen kann.

## Der Fix
`Quorum.reached(acks, current, old)` — **eine** reine Funktion, die alle Quorumsentscheidungen
beantwortet. `readIndex()`s Barriere und `hasValidLease()` gehen jetzt durch sie hindurch.
`advanceCommitIndex` bleibt unangetastet (es war korrekt) — aber es beantwortet nun nachweislich
dieselbe Frage.

Ein Detail mit Absicht: **`Quorum.reached` fügt `self` nicht implizit hinzu.** Ein Leader, der sich
gerade selbst entfernt, ist **kein** Mitglied von C_new und darf dort nicht mitzählen. Der Aufrufer
übergibt sich selbst, gezählt wird nur dort, wo er wirklich Mitglied ist.

## Warum eine reine Funktion
Eine Quorumsregel, die in drei Aufrufstellen verstreut in einem Knoten sitzt, lässt sich nur testen,
indem man einen Cluster hochfährt und ihn in eine Mitgliedschaftsänderung hineinrennen lässt. Also
wurde sie **nicht** getestet — und zwei von drei waren falsch. Als reine Funktion über
(Bestätigungen, C_old, C_new) ist sie eine Handvoll Assertions.

## Tests
`QuorumTest`: das Gegenbeispiel in beide Richtungen; beide Mehrheiten zusammen sind ein Quorum; der
überlappende Server zählt in beiden; ein Learner-Ack ist keine Stimme; ein Leader, der sich selbst
entfernt, zählt in C_new nicht mit; und — die Pointe — drei Bestätigungen können **kein** Quorum
sein, wenn sie in der falschen Konfiguration liegen. Grösse ist nicht die Frage, Mitgliedschaft ist es.

## Dateien
Neu: `Quorum.java`, `QuorumTest.java`. Geändert: `RaftNode` (`ReadBarrier` ohne Zählwert,
`checkReadBarriers`, `hasValidLease`).
