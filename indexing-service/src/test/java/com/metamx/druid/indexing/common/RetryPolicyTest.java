package com.metamx.druid.indexing.common;

import junit.framework.Assert;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Test;

/**
 */
public class RetryPolicyTest
{
  @Test
  public void testGetAndIncrementRetryDelay() throws Exception
  {
    RetryPolicy retryPolicy = new RetryPolicy(
        new RetryPolicyConfig()
            .setMinWait(new Period("PT1S"))
            .setMaxWait(new Period("PT10S"))
            .setMaxRetryCount(6)
    );

    Assert.assertEquals(new Duration("PT1S"), retryPolicy.getAndIncrementRetryDelay());
    Assert.assertEquals(new Duration("PT2S"), retryPolicy.getAndIncrementRetryDelay());
    Assert.assertEquals(new Duration("PT4S"), retryPolicy.getAndIncrementRetryDelay());
    Assert.assertEquals(new Duration("PT8S"), retryPolicy.getAndIncrementRetryDelay());
    Assert.assertEquals(new Duration("PT10S"), retryPolicy.getAndIncrementRetryDelay());
    Assert.assertEquals(new Duration("PT10S"), retryPolicy.getAndIncrementRetryDelay());
    Assert.assertEquals(null, retryPolicy.getAndIncrementRetryDelay());
    Assert.assertTrue(retryPolicy.hasExceededRetryThreshold());
  }
}
