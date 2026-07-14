# fbtberger-raft v113 — Storage-Backend konfigurierbar; Default ist WAL, inklusive Migration

## ⚠️ Warum das keine Konfigurationsänderung ist
Mit **abgeschalteten Snapshots** — so fährt kwatro — **IST der Log die Persistenz.** Ein
Backend-Wechsel ist deshalb eine **Datenmigration**.

Ein Knoten, der auf einem leeren WAL-Verzeichnis startet, hat einen leeren Log und eine **leere
State Machine**. Der Cluster meldet trotzdem `UP`, weil die übrigen Voter eine Mehrheit bilden. Das
ist exakt das Bild vom Juli — drei von fünf Knoten leer, alles sieht gesund aus —, nur diesmal selbst
herbeigeführt. Und ohne BerkeleyDB-Log gäbe es nichts, woraus es heilen könnte.

## Was geliefert wird
**`RaftStorageFactory`** — `raft.storage.type` = `wal` (neuer Default) | `bdb` | `memory`. Vorher war
das in **zwei** Spring-Konfigurationen (hier und in kwatro) hart verdrahtet.

> **Dass es konfigurierbar ist, wiegt schwerer als welcher Wert vorne steht.** Eine Wahl, die man nur
> durch Ändern zweier Quelldateien in zwei Repositories treffen kann, ist keine Wahl. Jetzt ist sie
> eine Property — und umkehrbar, falls die Benchmarks später etwas anderes sagen.

**`StorageMigration`** — kopiert den Log von einem `RaftStorage` in ein anderes: Einträge,
`currentTerm`, `votedFor`, Snapshot. Beim Wählen von `wal` läuft das **automatisch und in-place**,
wenn ein BDB-Log da ist und noch kein WAL. Idempotent — eine Migration, an die man bei fünf Knoten
einzeln denken muss, wird bei einem davon vergessen.

## Warum die Migration nur ~40 Zeilen braucht
Weil `RaftStorage` ein echtes Interface ist — und weil die **Storage-Contract-Suite (v106)**
nachweist, dass beide Implementierungen dieselben Invarianten erfüllen. Kopieren heisst dann: das eine
lesen, das andere schreiben. Das ist die Contract-Suite, die sich auszahlt.

## In-place und umkehrbar
BerkeleyDB schreibt `NNNNNNNN.jdb`, der WAL `wal-NNNNNN.log` — keine Kollision. Die Migration läuft im
vorhandenen Datenverzeichnis und **lässt die `.jdb`-Dateien liegen**. `storage.type=bdb` bringt den
alten Log zurück.

**Haltbarkeit:** Sobald der Knoten auf dem WAL läuft, ist der BDB-Log auf dem Migrationsstand
eingefroren; ein Rückwechsel verliert alles Spätere. Sicherheitsnetz für „das ging sofort schief",
kein Undo.

## Details, die zählen
- **Ein leeres Ziel ist Pflicht.** `copy()` verweigert ein Ziel mit Log — eine Migration ist kein
  Merge; einen Log über einen anderen zu schreiben ergäbe einen Log, den es nie gab.
- **Eine Lücke im Quell-Log bricht laut ab**, statt einen kürzeren Log zu schreiben und den Knoten
  „gesund" hochkommen zu lassen.
- **Ein unbekannter `storage.type` fliegt.** Stilles Zurückfallen auf den Default wäre das
  Schlimmste: ein Tippfehler in **einem** Knoten brächte ihn auf ein anderes Backend als seine Peers,
  und nichts würde es sagen.
- **`votedFor` wird mitkopiert** (§5.2) — die Zeile, die eine handgeschriebene Migration vergisst,
  und die einen Server zweimal im selben Term wählen lässt.

## Anmerkung zur Entscheidung
Die JMH-Benchmarks aus **v112** wurden für genau diese Frage gebaut (WAL vs. BDB) und sind **noch
nicht gelaufen**. Der Wechsel ist jetzt umkehrbar und verlustfrei — die Zahlen nachzureichen bleibt
trotzdem sinnvoll.

## Dateien
Neu: `RaftStorageFactory`, `StorageMigration`, `StorageMigrationTest`.
Geändert: `RaftNodeConfiguration` (Bean über die Factory).
