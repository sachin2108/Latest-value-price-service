
package com.example.prices;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Latest-value price service (in-memory).
 *
 * Functional requirements mapping:
 * --------------------------------
 * - Producers upload prices in batches
 * - Batches are isolated until completed
 * - Completion makes all prices visible atomically
 * - Consumers never see partial or cancelled batches
 * - Last value is chosen by `asOf`, not arrival order
 */
public final class LatestValuePriceService {

    /* ==================== DATA MODEL ==================== */

    /**
     * Represents a single price record.
     *
     * FR: The price data consists of records with id, asOf, payload
     */
    public static final class PriceRecord {
        public final String id;       // instrument identifier
        public final Instant asOf;     // producer-defined price time
        public final Object payload;   // flexible price data
        
        
      //Comstructor to initilize object
        public PriceRecord(String id, Instant asOf, Object payload) {
            this.id = Objects.requireNonNull(id);
            this.asOf = Objects.requireNonNull(asOf);
            this.payload = payload;
        }
    }

    /* ==================== INTERNAL STATE ==================== */

    /**
     * Immutable snapshot visible to consumers.
     *
     * FR: Consumers should only see data from previously completed batches
     * FR: Consumers should not see partial data
     */
    private volatile Map<String, PriceRecord> snapshot = Map.of();

    /**
     * Active producer batches.
     *
     * FR: Producers must upload prices in batches
     * FR: Uploads may happen in parallel
     */
    private final ConcurrentHashMap<UUID, ConcurrentHashMap<String, PriceRecord>> batches =
            new ConcurrentHashMap<>();

    /* ==================== PRODUCER API ==================== */

    /**
     * Starts a new batch.
     *
     * FR: The producer indicates that a batch is started
     */
    public UUID startBatch() {
        UUID id = UUID.randomUUID();
        batches.put(id, new ConcurrentHashMap<>());
        return id;
    }
    
    

    /**
     * Uploads price records for a batch.
     *
     * FR: Producers upload records in parallel
     * FR: Last value is determined by asOf time
     */
    public void upload(UUID batchId, Collection<PriceRecord> records) {
        ConcurrentHashMap<String, PriceRecord> batch = batches.get(batchId);
        if (batch == null) {
            throw new IllegalStateException("Batch not active: " + batchId);
        }

        for (PriceRecord r : records) {
            batch.merge(
                r.id,
                r,
                (oldV, newV) -> newV.asOf.isAfter(oldV.asOf) ? newV : oldV
            );
        }
    }

    /**
     * Cancels a batch and discards all uploaded data.
     *
     * FR: Cancelled batches are discarded entirely
     */
    public void cancelBatch(UUID batchId) {
        batches.remove(batchId);
    }

    /**
     * Completes a batch and atomically publishes its prices.
     *
     * FR: On completion, all prices in a batch become visible at the same time
     */
    public synchronized void completeBatch(UUID batchId) {
        Map<String, PriceRecord> batch = batches.remove(batchId);
        if (batch == null) {
            throw new IllegalStateException("Batch not active or already closed: " + batchId);
        }

        Map<String, PriceRecord> newSnapshot = new HashMap<>(snapshot);

        for (PriceRecord r : batch.values()) {
            newSnapshot.merge(
                r.id,
                r,
                (oldV, newV) -> newV.asOf.isAfter(oldV.asOf) ? newV : oldV
            );
        }

        snapshot = Map.copyOf(newSnapshot); // atomic publication
    }

    /* ==================== CONSUMER API ==================== */

    /**
     * Returns last prices for the given instrumeent IDs.
     *
     * FR: Consumers can request last prices
     * FR: No partial batch visibilty
     */
    public Map<String, PriceRecord> getLastPrices(Collection<String> ids) {
        Map<String, PriceRecord> view = snapshot;
        Map<String, PriceRecord> result = new HashMap<>(ids.size());

        for (String id : ids) {
            PriceRecord r = view.get(id);
            if (r != null) {
                result.put(id, r);
            }
        }
        return result;
    }
}
