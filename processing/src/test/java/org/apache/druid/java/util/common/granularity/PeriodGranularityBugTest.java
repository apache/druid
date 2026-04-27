package org.apache.druid.java.util.common.granularity;

import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class PeriodGranularityBugTest {
    @Test(timeout = 5000)
    public void testCompoundPeriodWithTimeZone() {
        // America/New_York has DST, so days are imprecise
        PeriodGranularity pg = new PeriodGranularity(new Period("PT1M1S"), null, DateTimeZone.forID("America/New_York"));
        long time = 1704067200000L; // 2024-01-01T00:00:00Z
        long start = System.currentTimeMillis();
        long bucket = pg.bucketStart(time);
        long end = System.currentTimeMillis();
        
        Assert.assertTrue("Should run quickly", (end - start) < 1000);
        
        // Let's also check if it returns the same as a non-compound period of same duration (PT61S)
        PeriodGranularity pg2 = new PeriodGranularity(new Period("PT61S"), null, DateTimeZone.forID("America/New_York"));
        long bucket2 = pg2.bucketStart(time);
        Assert.assertEquals(bucket2, bucket);
    }
}
