package io.ringbroker.benchmark;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Main benchmark suite runner for RingBroker performance testing.
 * This class serves as the entry point for running all benchmarks.
 */
public class Benchmarker {

    public static void main(final String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include("io.ringbroker.benchmark.*Benchmark")
                .exclude(Benchmarker.class.getSimpleName())
                .exclude(RawTcpClient.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
