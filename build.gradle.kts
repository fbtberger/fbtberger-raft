/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */

plugins {
    java
    jacoco
    id("com.google.protobuf") version "0.9.4"
    id("com.github.johnrengelman.shadow") version "8.1.1"
    // Mutation testing. jacoco above says a line ran; this says whether a test would have
    // noticed had the line been wrong.
    id("info.solidsoft.pitest") version "1.15.0"
}

// ── Mutation testing ──────────────────────────────────────────────────────────
//
//   ./gradlew pitest
//
// Pointed at the classes that DECIDE and nothing else. RaftNode is 2530 lines of threads,
// timers and sockets: one mutant there means booting a cluster, and the timing-dependent
// suites (ChaosTest, the three-node ones) would report survivors that are really timeouts.
// What is left is the arithmetic the whole protocol rests on — who is a majority, what a
// configuration parses to, what the log does when it is truncated.
// The two numbers, with a floor under each (quality-baseline.json). See gradle/ratchet.gradle.kts
// for what a ratchet is and how to raise one. This repository carries its own copy of that
// script: kwatro's is in a different checkout, and a build must not reach outside its own.
apply(from = file("gradle/ratchet.gradle.kts"))

// Coverage is cheap once JaCoCo has run, so its floor is part of `check`.
tasks.named("check") { dependsOn("coverageRatchet") }
// The mutation floor is not: a run here takes minutes, so it is checked when it is asked for.
tasks.named("pitest") { finalizedBy("mutationRatchet") }

pitest {
    junit5PluginVersion.set("1.2.0")
    targetClasses.set(listOf(
        "com.fbtberger.raft.Quorum*",
        "com.fbtberger.raft.ElectionSwitches*",
        "com.fbtberger.raft.RaftConfig*",
        "com.fbtberger.raft.HealthCheck*",
        "com.fbtberger.raft.KeyValueStateMachine*",
        "com.fbtberger.raft.InMemoryStorage*",
        "com.fbtberger.raft.StorageMigration*"
    ))
    targetTests.set(listOf(
        "com.fbtberger.raft.QuorumTest",
        "com.fbtberger.raft.ElectionSwitchesTest",
        // ElectionDefectSwitchTest is what actually pins both directions of the switches
        // (CLAUDE.md: the defect must really appear in front of an audience). Leaving it out
        // made ElectionSwitches look worse than it is.
        "com.fbtberger.raft.ElectionDefectSwitchTest",
        "com.fbtberger.raft.RaftConfigOfTest",
        "com.fbtberger.raft.HealthCheckTest",
        "com.fbtberger.raft.KeyValueStateMachineTest",
        "com.fbtberger.raft.KeyValueStateMachineCowTest",
        "com.fbtberger.raft.InMemoryStorageContractTest",
        "com.fbtberger.raft.StorageMigrationTest"
    ))
    outputFormats.set(listOf("XML", "HTML"))
    timestampedReports.set(false)
    threads.set(2)
}

group = "com.fbtberger"
version = "1.0.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

repositories {
    mavenCentral()
}

val grpcVersion = "1.62.2"
val protobufVersion = "3.25.3"
val springVersion = "6.1.6"
val micrometerVersion = "1.13.0"
val nettyVersion = "4.1.108.Final"

dependencies {
    // Protocol Buffers
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")

    // gRPC
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")

    // Berkeley DB
    implementation("com.sleepycat:je:18.3.12")

    // Spring IoC
    implementation("org.springframework:spring-context:$springVersion")
    compileOnly("org.springframework.boot:spring-boot-autoconfigure:3.2.5")

    // Micrometer metrics
    implementation("io.micrometer:micrometer-core:$micrometerVersion")
    implementation("io.micrometer:micrometer-registry-prometheus:$micrometerVersion")
    implementation("io.micrometer:micrometer-registry-jmx:$micrometerVersion")

    // Netty (raw TCP transport)
    implementation("io.netty:netty-transport:$nettyVersion")
    implementation("io.netty:netty-handler:$nettyVersion")
    implementation("io.netty:netty-codec:$nettyVersion")

    // Hadoop RPC transport
    implementation("org.apache.hadoop:hadoop-common:3.3.6") {
        exclude(group = "org.slf4j")
        exclude(group = "ch.qos.logback")
        exclude(group = "log4j")
        exclude(group = "org.apache.logging.log4j")
        exclude(group = "commons-logging")
        exclude(group = "org.eclipse.jetty")
        exclude(group = "org.apache.curator")
        exclude(group = "org.apache.zookeeper")
        exclude(group = "org.apache.avro")
        exclude(group = "com.sun.jersey")
        exclude(group = "javax.servlet")
    }

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.12")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.6")

    // Test
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.springframework:spring-test:$springVersion")
    testImplementation("io.grpc:grpc-inprocess:$grpcVersion")
    testImplementation("org.bouncycastle:bcpkix-jdk18on:1.78.1")
    testImplementation("org.awaitility:awaitility:4.2.1")
    testImplementation("org.openjdk.jmh:jmh-core:1.37")
    testAnnotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.37")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
    finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        csv.required.set(true)
        xml.required.set(true)
    }

    // Measure what someone wrote, not what protoc emitted.
    //
    // Two thirds of this repository by line count is generated: 6287 of 9113 lines live in
    // com.fbtberger.raft.proto and com.fbtberger.raft.client.proto, almost all of it in the
    // message classes and their $Builder. Measured on 2026-08-30, the generated part sat at
    // 30.9 % and the hand-written part at 81.3 %, and the two together produced the 46 % that
    // used to be the headline number. That number moved with the size of the .proto files
    // rather than with the tests, which is the opposite of what a ratchet is for.
    //
    // Raising it would have meant tests for AppendEntriesRequest.newBuilder().setTerm(1) --
    // which tests protoc. The build already draws exactly this line for the mutation run a few
    // dozen lines above ("Pointed at the classes that DECIDE and nothing else"); coverage simply
    // had not been told.
    classDirectories.setFrom(
        files(classDirectories.files.map {
            fileTree(it) { exclude("com/fbtberger/raft/**/proto/**", "com/fbtberger/raft/proto/**") }
        })
    )
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "com.fbtberger.raft.RaftServer"
    }
}

// gRPC finds its transports, resolvers and load balancers through the ServiceLoader, and every
// grpc artifact ships its own META-INF/services/io.grpc.NameResolverProvider. Without an explicit
// merge, shadow keeps whichever copy it unpacked last: the fat jar ended up declaring only
// UdsNameResolverProvider, so every node built from it died at startup with
//
//   Address types of NameResolver 'unix' for 'localhost:9093' not supported by transport
//
// -- i.e. the `java -jar ...-all.jar` path in the README could not have worked. The plain jar and
// the tests are unaffected, which is why this survived: nothing that runs in CI goes through the
// shaded artifact.
tasks.shadowJar {
    mergeServiceFiles()
}

// ---------------------------------------------------------------------------
// JMH benchmarks (StorageBenchmark)
//
// Deliberately NOT wired into `build`. They take minutes and hammer the disk, and a benchmark
// that fails the build for being 8% slower on a loaded laptop just teaches people to ignore the
// build. They are a measuring instrument, not a gate.
//
// jmh-core and its annotation processor are already on the test classpath, so the benchmarks live
// in src/test/java and need no separate source set:
//
//   ./gradlew jmh
//   ./gradlew jmh -Pjmh.args="recoverFromAnExistingLog"
//   ./gradlew jmh -Pjmh.args="-p impl=wal -p batchSize=10"
// ---------------------------------------------------------------------------
tasks.register<JavaExec>("jmh") {
    group = "verification"
    description = "Runs the JMH storage benchmarks (minutes, not seconds — see StorageBenchmark)."
    dependsOn(tasks.testClasses)
    mainClass.set("org.openjdk.jmh.Main")
    classpath = sourceSets["test"].runtimeClasspath

    // Default to the storage benchmarks; -Pjmh.args overrides entirely.
    val extra = (project.findProperty("jmh.args") as String?) ?: "StorageBenchmark"
    args = extra.split(" ").filter { it.isNotBlank() }
}
