# fbtberger-raft v107 — Der Health-Endpunkt sagt, ob ein Knoten stimmberechtigt ist

## Warum
Jeder Nicht-Leader meldete `follower`. kwatro-4 und kwatro-5 sind **Learner** — sie stimmen nicht
ab —, waren von aussen aber nicht von den drei Votern zu unterscheiden:

```
kwatro-1: {"status":"UP","message":"follower, leader=kwatro2"}   ← Voter
kwatro-4: {"status":"UP","message":"follower, leader=kwatro2"}   ← Learner
```

Das ist nicht kosmetisch. **Ein fehlender Voter frisst am Quorum; ein fehlender Learner kostet nur
Lesekapazität.** Im Juli-Ausfall standen drei von fünf Knoten mit **leerer State Machine** da — und
„welche davon sind Voter?" war die erste Frage, die zählte. Genau die konnte der Health-Endpunkt
nicht beantworten.

## Was NICHT geändert wurde
**`ServerRole` bleibt bei `FOLLOWER`/`CANDIDATE`/`LEADER`.** Ein Learner ist **keine vierte Rolle**:
nach §4.2.1 ist er ein nicht stimmberechtigtes **Mitglied** und rollentechnisch ein Follower wie
jeder andere. Mitgliedschaft und Rolle sind zwei verschiedene Fragen; sie ineinanderzufalten würde
den Algorithmus falsch darstellen. Die Rolle bleibt exakt wie in Figure 4, das Learner-Flag läuft
**daneben** mit — `RaftNode.isLearner()` gab es ohnehin schon.

## Neue Meldungen
| Lage | vorher | jetzt |
|---|---|---|
| Learner, gesund | `follower, leader=kwatro2` | **`learner, leader=kwatro2`** |
| Voter, gesund | `follower, leader=kwatro2` | `follower, leader=kwatro2` (unverändert) |
| Learner ohne Leader-Kontakt | `no recent leader contact` | **`learner, no recent leader contact`** |
| Learner holt auf | `behind: applied=…` | **`learner behind: applied=…`** |

## Tests
`HealthCheckTest`: ein gesunder Learner meldet `learner`, ein stimmberechtigter Follower weiterhin
`follower`; ein Learner ohne Leader-Kontakt bleibt als Learner erkennbar. Und ein Test hält fest,
dass `ServerRole` **drei** Werte hat — damit „LEARNER als vierte Rolle" nicht versehentlich
zurückkommt.

## Signatur
`HealthCheck.readinessStatus(...)` bekommt ein `boolean learner` (package-private, nur intern und
im Test verwendet).
