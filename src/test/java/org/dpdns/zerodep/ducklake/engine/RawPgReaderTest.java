package org.dpdns.zerodep.ducklake.engine;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RawPgReaderTest {

    @Test
    void convertsPgEpochMicrosToUnixMillis() {
        assertThat(RawPgReader.pgTimestampMs(0)).isEqualTo(946_684_800_000L);
        assertThat(RawPgReader.pgTimestampMs(1_234_567)).isEqualTo(946_684_801_234L);
        assertThat(RawPgReader.pgTimestampMs(-1_000)).isEqualTo(946_684_799_999L);
    }
}
