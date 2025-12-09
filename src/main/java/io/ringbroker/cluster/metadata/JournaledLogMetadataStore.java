package io.ringbroker.cluster.metadata;

import lombok.extern.slf4j.Slf4j;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Durable, serialized metadata store backed by a local write-ahead journal.
 * Single-writer per process; wrap with a broadcaster/leader to share updates.
 */
@Slf4j
public final class JournaledLogMetadataStore implements LogMetadataStore {
    private static final String FILE_PREFIX = "partition-";
    private static final String FILE_SUFFIX = ".meta";
    private final Path dir;
    private final Map<Integer, PartitionState> byPartition = new ConcurrentHashMap<>();
    public JournaledLogMetadataStore(final Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir);
    }

    @Override
    public Optional<LogConfiguration> current(final int partitionId) {
        final PartitionState st = loadOrCreate(partitionId);
        st.lock.readLock().lock();
        try {
            return Optional.ofNullable(st.config);
        } finally {
            st.lock.readLock().unlock();
        }
    }

    @Override
    public LogConfiguration bootstrapIfAbsent(final int partitionId,
                                              final EpochPlacement placement,
                                              final long startSeqInclusive) {
        final PartitionState st = loadOrCreate(partitionId);
        st.lock.writeLock().lock();
        try {
            if (st.config != null) return st.config;

            final EpochMetadata epoch0 = new EpochMetadata(
                    placement.getEpoch(),
                    startSeqInclusive,
                    -1L,
                    placement,
                    /* tieBreaker */ 0L
            );
            st.config = new LogConfiguration(partitionId, 1L, List.of(epoch0));
            persist(partitionId, st.config);
            return st.config;
        } catch (final IOException ioe) {
            throw new RuntimeException("Failed to bootstrap metadata for partition " + partitionId, ioe);
        } finally {
            st.lock.writeLock().unlock();
        }
    }

    @Override
    public LogConfiguration sealAndCreateEpoch(final int partitionId,
                                               final long activeEpoch,
                                               final long sealedEndSeq,
                                               final EpochPlacement newPlacement,
                                               final long newEpochId,
                                               final long tieBreaker) {
        final PartitionState st = loadOrCreate(partitionId);
        st.lock.writeLock().lock();
        try {
            if (st.config == null) {
                throw new IllegalStateException("No configuration exists for partition " + partitionId);
            }

            final LogConfiguration cfg = st.config;
            final EpochMetadata current = cfg.activeEpoch();
            if (current.epoch() != activeEpoch) {
                throw new IllegalStateException("Active epoch mismatch. expected=" + activeEpoch +
                        " actual=" + current.epoch());
            }

            final LogConfiguration sealed = cfg.sealActive(sealedEndSeq);
            final long nextStartSeq = sealedEndSeq + 1;
            final EpochMetadata nextMeta = new EpochMetadata(
                    newEpochId,
                    nextStartSeq,
                    -1L,
                    newPlacement.withEpoch(newEpochId),
                    tieBreaker
            );

            st.config = sealed.appendEpoch(nextMeta);
            persist(partitionId, st.config);
            return st.config;
        } catch (final IOException ioe) {
            throw new RuntimeException("Failed to persist metadata for partition " + partitionId, ioe);
        } finally {
            st.lock.writeLock().unlock();
        }
    }

    @Override
    public void applyRemote(final LogConfiguration cfg) {
        final int pid = cfg.partitionId();
        final PartitionState st = loadOrCreate(pid);
        st.lock.writeLock().lock();
        try {
            if (st.config == null || cfg.configVersion() > st.config.configVersion()) {
                st.config = cfg;
                persist(pid, cfg);
            }
        } catch (final IOException ioe) {
            throw new RuntimeException("Failed to persist remote metadata for partition " + pid, ioe);
        } finally {
            st.lock.writeLock().unlock();
        }
    }

    private PartitionState loadOrCreate(final int partitionId) {
        return byPartition.computeIfAbsent(partitionId, pid -> {
            final PartitionState ps = new PartitionState();
            try {
                ps.config = readFromDisk(pid);
            } catch (final IOException ioe) {
                log.warn("Failed to load metadata for partition {}: {}", pid, ioe.toString());
            }
            return ps;
        });
    }

    private PartitionState loadPartition(final int partitionId) {
        return byPartition.get(partitionId);
    }

    private Path fileFor(final int pid) {
        return dir.resolve(FILE_PREFIX + pid + FILE_SUFFIX);
    }

    private LogConfiguration readFromDisk(final int pid) throws IOException {
        final Path f = fileFor(pid);
        if (!Files.exists(f)) return null;

        try (final DataInputStream in = new DataInputStream(Files.newInputStream(f))) {
            final long cfgVersion = in.readLong();
            final int epochs = in.readInt();
            final List<EpochMetadata> list = new ArrayList<>(epochs);
            for (int i = 0; i < epochs; i++) {
                final long epoch = in.readLong();
                final long startSeq = in.readLong();
                final long endSeq = in.readLong();
                final long tieBreaker = in.readLong();
                final int ackQuorum = in.readInt();
                final int replicaCount = in.readInt();
                final List<Integer> replicas = new ArrayList<>(replicaCount);
                for (int r = 0; r < replicaCount; r++) {
                    replicas.add(in.readInt());
                }
                final EpochPlacement placement = new EpochPlacement(epoch, replicas, ackQuorum);
                list.add(new EpochMetadata(epoch, startSeq, endSeq, placement, tieBreaker));
            }
            return new LogConfiguration(pid, cfgVersion, list);
        }
    }

    private void persist(final int pid, final LogConfiguration cfg) throws IOException {
        final Path tmp = fileFor(pid).resolveSibling(fileFor(pid).getFileName().toString() + ".tmp");
        try (final DataOutputStream out = new DataOutputStream(Files.newOutputStream(tmp))) {
            out.writeLong(cfg.configVersion());
            final List<EpochMetadata> epochs = cfg.epochs();
            out.writeInt(epochs.size());
            for (final EpochMetadata em : epochs) {
                out.writeLong(em.epoch());
                out.writeLong(em.startSeq());
                out.writeLong(em.endSeq());
                out.writeLong(em.tieBreaker());
                final EpochPlacement p = em.placement();
                out.writeInt(p.getAckQuorum());
                final List<Integer> nodes = p.getStorageNodes();
                out.writeInt(nodes.size());
                for (final Integer n : nodes) {
                    out.writeInt(n);
                }
            }
            out.flush();
        }
        Files.move(tmp, fileFor(pid), java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }

    private static final class PartitionState {
        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        LogConfiguration config;
    }
}
