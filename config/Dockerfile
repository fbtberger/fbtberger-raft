FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# The Raft configuration for all three nodes, copied in together
COPY config/ /app/config/

# The built jar (./gradlew shadowJar)
COPY build/libs/*-all.jar app.jar

# The Raft data directory as a volume — each node has its own
VOLUME /var/raft

# Which properties file to start with. Overridden per node in docker-compose.yml
# through RAFT_CONFIG_PATH; the default here is only what a bare `docker run` gets.
ENV RAFT_CONFIG_PATH=/app/config/node1.properties

# Extra JVM options, e.g. -Draft.cli.enabled=false for a container without a terminal
# (docker run -d, docker compose): without it the node stops when stdin ends.
ENV JAVA_OPTS=""

# exec, so the JVM is PID 1 and receives SIGTERM: the node then closes its storage
# before the container stops instead of being killed after the grace period.
ENTRYPOINT ["sh", "-c", "exec java ${JAVA_OPTS} -jar /app/app.jar ${RAFT_CONFIG_PATH}"]
