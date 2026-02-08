
package com.example.prices;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class LatestValuePriceServiceTest {

    @Test
    void completedBatchIsVisibleAtomically() {
        LatestValuePriceService service = new LatestValuePriceService();
        UUID batch = service.startBatch();

        service.upload(batch, List.of(
            new LatestValuePriceService.PriceRecord("A", Instant.parse("2024-01-01T00:00:00Z"), 100),
            new LatestValuePriceService.PriceRecord("B", Instant.parse("2024-01-01T00:00:00Z"), 200)
        ));

        assertTrue(service.getLastPrices(List.of("A", "B")).isEmpty());

        service.completeBatch(batch);

        assertEquals(2, service.getLastPrices(List.of("A", "B")).size());
    }

    @Test
    void lastAsOfWins() {
        LatestValuePriceService service = new LatestValuePriceService();
        UUID batch = service.startBatch();

        service.upload(batch, List.of(
            new LatestValuePriceService.PriceRecord("A", Instant.parse("2024-01-01T00:00:00Z"), 100),
            new LatestValuePriceService.PriceRecord("A", Instant.parse("2024-01-02T00:00:00Z"), 200)
        ));

        service.completeBatch(batch);

        assertEquals(200,
            service.getLastPrices(List.of("A")).get("A").payload);
    }
}
