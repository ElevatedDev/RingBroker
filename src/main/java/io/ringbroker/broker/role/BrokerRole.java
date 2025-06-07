package io.ringbroker.broker.role;

/**
 * Enumerates the two operational modes a RingBroker node can run in: a lightweight
 * {@link #INGESTION} front‑door or a stateful {@link #PERSISTENCE} storage node.
 */
public enum BrokerRole {
    /**
     * Parses, validates and forwards messages to persistence brokers.
     */
    INGESTION,
    /**
     * Owns ledger segments and provides durable storage for a partition subset.
     */
    PERSISTENCE
}