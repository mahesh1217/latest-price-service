package com.latestprice.latest_price_service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;

/**
 * In-memory implementation of a Latest Price Service.
 *
 * Guarantees:
 * - Atomic batch visibility
 * - No partial reads
 * - Last-value determined by asOf timestamp
 * - Thread-safe
 */
public class LatestPriceServiceTest {

	@Test
    void testAtomicBatchVisibility() {
        LatestPriceService service = new LatestPriceService();

        UUID batch = service.startBatch();

        service.uploadPrices(batch, List.of(
                new PriceRecord("A", Instant.parse("2024-01-01T10:00:00Z"), 100)
        ));

        assertTrue(service.getLatestPrices(List.of("A")).isEmpty());

        service.completeBatch(batch);

        Map<String, PriceRecord> result = service.getLatestPrices(List.of("A"));
        assertEquals(1, result.size());
        assertEquals(100, result.get("A").getPayload());
    }

    @Test
    void testLastValueByAsOf() {
        LatestPriceService service = new LatestPriceService();

        UUID batch = service.startBatch();

        service.uploadPrices(batch, List.of(
                new PriceRecord("A", Instant.parse("2024-01-01T09:00:00Z"), 90),
                new PriceRecord("A", Instant.parse("2024-01-01T10:00:00Z"), 100)
        ));

        service.completeBatch(batch);

        Map<String, PriceRecord> result = service.getLatestPrices(List.of("A"));
        assertEquals(100, result.get("A").getPayload());
    }

    @Test
    void testCancelBatch() {
        LatestPriceService service = new LatestPriceService();

        UUID batch = service.startBatch();
        service.uploadPrices(batch, List.of(
                new PriceRecord("A", Instant.now(), 100)
        ));

        service.cancelBatch(batch);

        assertTrue(service.getLatestPrices(List.of("A")).isEmpty());
    }

    @Test
    void testMultipleBatches() {
        LatestPriceService service = new LatestPriceService();

        UUID batch1 = service.startBatch();
        service.uploadPrices(batch1, List.of(
                new PriceRecord("A", Instant.parse("2024-01-01T10:00:00Z"), 100)
        ));
        service.completeBatch(batch1);

        UUID batch2 = service.startBatch();
        service.uploadPrices(batch2, List.of(
                new PriceRecord("A", Instant.parse("2024-01-02T10:00:00Z"), 200)
        ));
        service.completeBatch(batch2);

        Map<String, PriceRecord> result = service.getLatestPrices(List.of("A"));
        assertEquals(200, result.get("A").getPayload());
    }
    
    @Test
    void testChunkSizeValidation() {
        LatestPriceService service = new LatestPriceService();
        UUID batch = service.startBatch();

        List<PriceRecord> tooLargeChunk =
                java.util.stream.IntStream.range(0, 1001)
                        .mapToObj(i -> new PriceRecord(
                                "ID" + i,
                                java.time.Instant.now(),
                                i))
                        .toList();

        assertThrows(IllegalArgumentException.class, () ->
                service.uploadPrices(batch, tooLargeChunk)
        );
    }

    
}
