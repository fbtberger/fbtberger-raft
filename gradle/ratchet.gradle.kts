// A ratchet for the two numbers that say how good the tests are.
//
// Coverage says a line ran. Mutation testing says a test would have NOTICED had the line been
// wrong. Both are recorded in quality-baseline.json next to the build file, and both may only
// go UP: a run below the recorded floor fails the build, a run above it prints what to write
// down. That is the whole idea — not "reach 80 %", which nobody ever does in one step, but
// "never quietly go backwards", which is a thing a build can enforce.
//
//   ./gradlew ratchet                     # check both numbers against the floor
//   ./gradlew ratchet -PupdateBaseline    # raise the floor to what was just measured
//
// The coverage half is wired into `check`, because JaCoCo already ran with the tests and
// comparing two numbers costs nothing. The mutation half is NOT: a mutation run takes minutes
// (HANDOVER §4.5 says so deliberately), so it is checked when the mutation task itself runs.
//
// Applied with `apply(from = ...)` rather than made a plugin: it is sixty lines, it is used by
// three builds, and one of them (fbtberger-raft) is a different repository that carries its own
// copy. A buildSrc plugin would be a published artifact for the sake of a JSON file.

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

/** Where the floor is written down, and what is in it. */
val baselineFile = file("quality-baseline.json")

/** Raising the floor is an explicit act, never a side effect of a good run. */
val updating = providers.gradleProperty("updateBaseline").isPresent

/**
 * How much better than the floor a run has to be before it is worth mentioning. Coverage moves
 * by fractions of a percent when a single line is added, and a ratchet that demanded a bump for
 * every one of those would be a ratchet nobody could commit through.
 */
val slack = 0.005

fun readBaseline(): MutableMap<String, Any> {
    if (!baselineFile.exists()) return mutableMapOf()
    @Suppress("UNCHECKED_CAST")
    return (JsonSlurper().parse(baselineFile) as Map<String, Any>).toMutableMap()
}

fun writeBaseline(data: Map<String, Any>) {
    baselineFile.writeText(JsonOutput.prettyPrint(JsonOutput.toJson(data)) + "\n")
}

/**
 * Compare one measured ratio against its floor.
 *
 * @return the value to record, which is the measured one when it improved and the old floor
 *         otherwise — so `-PupdateBaseline` after a bad run cannot silently lower the bar.
 */
fun ratchet(name: String, measured: Double, floor: Double?): Double {
    val pct = { d: Double -> String.format("%.2f %%", d * 100) }

    if (floor == null) {
        logger.lifecycle("ratchet: $name has no floor yet, measured ${pct(measured)}")
        return measured
    }
    if (measured < floor - 1e-9) {
        throw GradleException(
            "ratchet: $name fell from ${pct(floor)} to ${pct(measured)}.\n" +
            "  Either put the tests back, or — if the drop is intended and understood — " +
            "lower the number in ${baselineFile.name} by hand, in a commit that says why."
        )
    }
    if (measured > floor + slack) {
        logger.lifecycle(
            "ratchet: $name rose from ${pct(floor)} to ${pct(measured)}" +
            if (updating) " — recorded." else " — run with -PupdateBaseline to record it."
        )
        return measured
    }
    logger.lifecycle("ratchet: $name holds at ${pct(measured)} (floor ${pct(floor)})")
    return floor
}

/**
 * A parser for the two report formats.
 *
 * Both declarations matter. Groovy's XmlParser refuses a DOCTYPE outright by default, and a
 * JaCoCo report opens with one — so reading it fails with "DOCTYPE is disallowed" before a
 * single counter is seen. Allowing the declaration is not the same as fetching it: the second
 * feature keeps the parser from going to jacoco.org for the DTD, because a build that needs the
 * network to read a local file is a build that fails on a train.
 */
fun xmlParser(): groovy.xml.XmlParser = groovy.xml.XmlParser(false, false).apply {
    setFeature("http://apache.org/xml/features/disallow-doctype-decl", false)
    setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
}

/** LINE counters from a JaCoCo XML report, summed over every top-level counter in it. */
fun jacocoLineRatio(report: File): Double? {
    if (!report.exists()) return null
    val root = xmlParser().parse(report)
    val line = root.children()
        .filterIsInstance<groovy.util.Node>()
        .firstOrNull { it.name() == "counter" && it.attribute("type") == "LINE" }
        ?: return null
    val covered = (line.attribute("covered") as String).toDouble()
    val missed = (line.attribute("missed") as String).toDouble()
    return if (covered + missed == 0.0) null else covered / (covered + missed)
}

/** KILLED over total, from a Pitest XML report. */
fun pitestKillRatio(report: File): Double? {
    if (!report.exists()) return null
    val mutations = xmlParser().parse(report).children().filterIsInstance<groovy.util.Node>()
    if (mutations.isEmpty()) return null
    val killed = mutations.count { it.attribute("status") == "KILLED" }
    return killed.toDouble() / mutations.size
}

// ── The tasks ─────────────────────────────────────────────────────────────────

val coverageRatchet by tasks.registering {
    group = "verification"
    description = "Fails if line coverage dropped below quality-baseline.json."

    // JaCoCo's own default, unless the build says otherwise — the Android plugin writes its
    // report somewhere else entirely, so a build may set `ext["ratchetCoverageReport"]` to a
    // path relative to its build directory BEFORE applying this script.
    val relative = (project.extra.properties["ratchetCoverageReport"] as? String)
        ?: "reports/jacoco/test/jacocoTestReport.xml"
    val report = layout.buildDirectory.file(relative)
    doLast {
        val measured = jacocoLineRatio(report.get().asFile)
            ?: throw GradleException(
                "ratchet: no JaCoCo report at ${report.get().asFile}. " +
                "Run the tests first — a ratchet with no measurement is not a pass."
            )
        val data = readBaseline()
        @Suppress("UNCHECKED_CAST")
        val coverage = (data["coverage"] as? Map<String, Any>)?.toMutableMap() ?: mutableMapOf()
        val kept = ratchet("coverage (line)", measured, (coverage["line"] as? Number)?.toDouble())
        if (updating) {
            coverage["line"] = kept
            data["coverage"] = coverage
            writeBaseline(data)
        }
    }
}

val mutationRatchet by tasks.registering {
    group = "verification"
    description = "Fails if the mutation score dropped below quality-baseline.json."

    val report = layout.buildDirectory.file("reports/pitest/mutations.xml")
    doLast {
        val measured = pitestKillRatio(report.get().asFile)
            ?: throw GradleException(
                "ratchet: no Pitest report at ${report.get().asFile}. Run the mutation task first."
            )
        val data = readBaseline()
        @Suppress("UNCHECKED_CAST")
        val mutation = (data["mutation"] as? Map<String, Any>)?.toMutableMap() ?: mutableMapOf()
        val kept = ratchet("mutation score", measured, (mutation["score"] as? Number)?.toDouble())
        if (updating) {
            mutation["score"] = kept
            data["mutation"] = mutation
            writeBaseline(data)
        }
    }
}

tasks.register("ratchet") {
    group = "verification"
    description = "Both quality floors: coverage and mutation score."
    dependsOn(coverageRatchet, mutationRatchet)
}
