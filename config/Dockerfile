FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# Raft-Konfigurationsdateien für alle 3 Nodes mitkopieren
COPY config/ /app/config/

# Gebautes JAR (via ./gradlew shadowJar)
COPY build/libs/*-all.jar app.jar

# Raft-Datenpfad als Volume (jeder Node hat seinen eigenen)
VOLUME /var/raft

# Welche properties-Datei soll gestartet werden?
# Wird im docker-compose.yml per RAFT_CONFIG_PATH überschrieben.
ENV RAFT_CONFIG_PATH=/app/config/node1.properties

ENTRYPOINT ["sh", "-c", "java -jar /app/app.jar ${RAFT_CONFIG_PATH}"]
