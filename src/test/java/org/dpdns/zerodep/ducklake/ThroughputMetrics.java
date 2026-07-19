package org.dpdns.zerodep.ducklake;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.dpdns.zerodep.ducklake.metrics.SyncState;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

final class ThroughputMetrics {

    private ThroughputMetrics() {
    }

    static void printAndAssert(MeterRegistry registry, SyncState.Reader reader) {
        String readerTag = reader.name().toLowerCase(Locale.ROOT);
        for (SyncState.Stage stage : SyncState.Stage.values()) {
            String stageTag = stage.name().toLowerCase(Locale.ROOT);
            Timer timer = registry.get("ducklake_batch_stage_duration")
                    .tags("reader", readerTag, "stage", stageTag).timer();
            assertThat(timer.count()).as(readerTag + ' ' + stageTag + " timer count").isPositive();
            var snapshot = timer.takeSnapshot();
            System.out.printf("[STAGE] reader=%s stage=%-12s count=%d p50=%.3fms p95=%.3fms max=%.3fms%n",
                    readerTag, stageTag, timer.count(), percentileMs(snapshot, 0.5),
                    percentileMs(snapshot, 0.95), snapshot.max(TimeUnit.MILLISECONDS));
        }
    }

    static void assertSourceTiming(SyncState state, long sourceStartedAtMs, long finishedAtMs) {
        assertThat(state.getLastSourceTsMs().get())
                .isBetween(sourceStartedAtMs - 1_000L, finishedAtMs);
        assertThat(state.getLastDeliverLagMs().get()).isNotNegative();
        assertThat(state.getLastBatchLagMs().get()).isGreaterThanOrEqualTo(
                state.getLastDeliverLagMs().get());
    }

    private static double percentileMs(io.micrometer.core.instrument.distribution.HistogramSnapshot snapshot,
                                       double percentile) {
        for (var value : snapshot.percentileValues()) {
            if (Double.compare(value.percentile(), percentile) == 0) {
                return value.value(TimeUnit.MILLISECONDS);
            }
        }
        return Double.NaN;
    }
}
