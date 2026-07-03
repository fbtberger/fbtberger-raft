plugins {
    java
    jacoco
    id("com.google.protobuf") version "0.9.4"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "com.fbtberger.raft"
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
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "com.fbtberger.raft.RaftServer"
    }
}
