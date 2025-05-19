package io.ringbroker.retention;

import io.ringbroker.ledger.constant.LedgerConstant;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Background job that periodically:
 * - Deletes segment files older than `maxAge`
 * - Ensures total bytes per topic &lt;= `maxBytes` by oldest-first deletion
 */
@Slf4j
@Deprecated
public final class RetentionManager {
    private final Path baseDir;
    private final Duration maxAge;
    private final long maxBytes;

    public RetentionManager(final Path baseDir, final Duration maxAge, final long maxBytes) {
        this.baseDir = baseDir;
        this.maxAge = maxAge;
        this.maxBytes = maxBytes;
    }

    public void start() {
        Executors.newSingleThreadScheduledExecutor(
                        r -> Thread.ofVirtual().factory().newThread(r))
                .scheduleAtFixedRate(this::sweep, 1, 1, TimeUnit.HOURS);
    }

    private void sweep() {
        try {
            Files.list(baseDir).filter(Files::isDirectory).forEach(this::cleanTopic);
        } catch (final IOException e) {
            log.error("Retention sweep failed", e);
        }
    }

    private void cleanTopic(final Path topicDir) {
        try {
            // gather segment files
            final var segs = Files.list(topicDir)
                    .filter(p -> p.getFileName().toString().endsWith(LedgerConstant.SEGMENT_EXT))
                    .sorted(Comparator.comparingLong(p -> p.toFile().lastModified()))
                    .toList();

            // delete by age
            final Instant cutoff = Instant.now().minus(maxAge);
            for (final Path seg : segs) {
                final FileTime mt = Files.getLastModifiedTime(seg);

                if (mt.toInstant().isBefore(cutoff)) {
                    Files.delete(seg);
                    log.info("Deleted old segment {}", seg);
                }
            }

            // enforce maxBytes
            long total = segs.stream()
                    .mapToLong(p -> p.toFile().length())
                    .sum();
            for (final Path seg : segs) {
                if (total <= maxBytes) break;

                final long len = seg.toFile().length();
                Files.delete(seg);
                total -= len;

                log.info("Deleted segment {} to enforce size limit", seg);
            }

        } catch (final IOException e) {
            log.error("Failed retention for {}", topicDir, e);
        }
    }
}
