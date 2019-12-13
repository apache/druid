package org.apache.druid.indexing;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.client.indexing.ClientCompactQueryTuningConfig;
import org.junit.Test;

public class ClientCompactQueryTuningConfigTest {
    @Test
    public void test_equalsContract() {
        // If this test failed, make sure to validate that toString was also updated correctly!
        EqualsVerifier.forClass(ClientCompactQueryTuningConfig.class).usingGetClass().verify();
    }
}
