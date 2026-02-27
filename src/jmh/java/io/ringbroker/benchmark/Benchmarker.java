package io.ringbroker.benchmark;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Main benchmark suite runner for RingBroker performance testing.
 * This class serves as the entry point for running all benchmarks.
 */
public class Benchmarker {

    public static void main(final String[] args) throws RunnerException {
        final boolean isWindows = System.getProperty("os.name", "").toLowerCase().contains("win");

        // Where all profiler artifacts go (JFR or async-profiler outputs).
        final Path outDir = Paths.get(System.getProperty(
                "ringbroker.profile.dir",
                System.getProperty("ringbroker.async.dir", "build/reports/jmh/profile")
        )).toAbsolutePath().normalize();
        ensureDirectory(outDir);

        final OptionsBuilder builder = new OptionsBuilder();
        builder.include("io.ringbroker.benchmark.*Benchmark");
        builder.exclude(Benchmarker.class.getSimpleName());
        builder.exclude(RawTcpClient.class.getSimpleName());

        // Ensure forks also get these (NOT just the JavaExec runner JVM)
        builder.jvmArgsAppend(
                "--enable-preview",
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+DebugNonSafepoints"
        );

        // Pick profiler backend:
        // - Windows: use JFR (built-in, no native DLL needed)
        // - Others: use async-profiler (as you had)
        if (isWindows) {
            final String jfrSettings = System.getProperty("ringbroker.jfr.settings", "profile");
            final int stackDepth = Integer.getInteger("ringbroker.jfr.stackdepth", 256);

            // JFR supports %p (pid) and %t (timestamp) filename expansion. :contentReference[oaicite:2]{index=2}
            final String jfrFile = outDir.resolve("ringbroker-%p-%t.jfr").toString();

            builder.jvmArgsAppend(
                    "-XX:StartFlightRecording=filename=" + jfrFile + ",settings=" + jfrSettings,
                    "-XX:FlightRecorderOptions=stackdepth=" + stackDepth
            );
        } else {
            final String asyncLibPath = firstNonBlank(
                    System.getProperty("ringbroker.async.libPath"),
                    System.getenv("ASYNC_PROFILER_LIB"),
                    "/opt/async-profiler/lib/libasyncProfiler.so"
            );

            // Important: JMH async profiler options are separated by ';' not ','.
            final String asyncProfilerOptions = String.join(";",
                    "libPath=" + asyncLibPath,
                    "event=" + System.getProperty("ringbroker.async.event", "cpu"),
                    "output=" + System.getProperty("ringbroker.async.output", "flamegraph"),
                    "dir=" + outDir
            );

            builder.addProfiler("async", asyncProfilerOptions);
        }

        if (Boolean.getBoolean("ringbroker.profile.gc")) {
            builder.addProfiler("gc");
        }

        final Options opt = builder.build();
        new Runner(opt).run();
    }

    private static String firstNonBlank(final String... values) {
        for (final String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        throw new IllegalStateException("No non-blank value provided");
    }

    private static void ensureDirectory(final Path path) {
        try {
            Files.createDirectories(path);
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to create profiler output directory: " + path, e);
        }
    }
}