package org.dpdns.zerodep.ducklake.metrics;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SyncStateTest {

    @Test
    void recordsFixedCardinalityStageTimersAndFailures() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SyncState state = new SyncState(registry);

        state.recordStage(SyncState.Reader.POSTGRES, SyncState.Stage.DECODE,
                TimeUnit.MILLISECONDS.toNanos(7));
        state.recordStage(SyncState.Reader.POSTGRES, SyncState.Stage.DECODE, 0);
        state.batchFailed();
        state.batchCommitted(1, System.currentTimeMillis() + 10_000L, 0, 1, 2);

        Timer timer = registry.get("ducklake_batch_stage_duration")
                .tags("reader", "postgres", "stage", "decode").timer();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(7);
        assertThat(state.getBatchFailures().count()).isEqualTo(1);
        assertThat(state.getLastBatchLagMs().get()).isZero();
        assertThat(registry.find("ducklake_batch_stage_duration").timers()).hasSize(20);
    }
}
