# fbtberger-raft v103 — Der Empfänger schweigt nicht mehr (v102 war nicht kompilierbar)

## Anlass
Der Leader meldet (dank v101) unablässig:
```
kwatro1: AppendEntries fehlgeschlagen (prevLogIndex=0, entries=344): StatusRuntimeException: UNKNOWN
```
`UNKNOWN` heisst in gRPC: **der Empfänger hat eine unbehandelte Exception geworfen.** Im Log des
Empfängers steht darüber — **nichts**.

## Ursache der Unsichtbarkeit
`GrpcTransportServer`:
```java
} catch (RuntimeException e) { observer.onError(e); }
```
Die Exception wird beantwortet, aber **nirgends protokolliert**. Nur der Empfänger kennt die
Ursache, und genau er sagt nichts. Ergebnis: eine Endlosschleife, die von aussen nicht
aufzuklären ist — der Follower lehnt den Log ab, der Leader setzt `nextIndex` auf 1 zurück,
schickt alle 344 Einträge erneut, der Empfänger wirft wieder, und so weiter. Genau das hat
kwatro-1, kwatro-4 und kwatro-5 seit heute Morgen leer gehalten.

## Änderung
Jeder RPC-Handler protokolliert die Exception mit vollem Stacktrace, bevor er sie beantwortet.
Bei `AppendEntries` zusätzlich der Kontext, der den Fall reproduzierbar macht:
```
ERROR Raft-RPC AppendEntries fehlgeschlagen (prevLogIndex=0 entries=344 leaderCommit=344 term=40)
  java.lang.…: …
      at com.fbtberger.raft.storage.…
```

**Kein Verhaltenswechsel** — Instrumentierung. Zusammen mit v101 ist damit beides sichtbar: dass
repliziert wird und woran es scheitert.

## Dateien
`src/main/java/com/fbtberger/raft/transport/GrpcTransportServer.java`.

## Nachtrag zu v102
v102 war **kaputt**: die `fail()`-Methode wurde durch einen fehlgeschlagenen Einfüge-Anker gar
nicht erst in die Klasse geschrieben — nur die Aufrufe. Compile-Fehler („cannot find symbol").
v103 enthält beides. Der Build ist das Gate; er hat korrekt gestoppt.
