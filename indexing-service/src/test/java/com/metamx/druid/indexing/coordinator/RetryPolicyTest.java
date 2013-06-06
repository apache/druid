package com.metamx.druid.indexing.coordinator;

import com.metamx.druid.indexing.common.RetryPolicy;
import com.metamx.druid.indexing.common.config.RetryPolicyConfig;
import junit.framework.Assert;
import org.joda.time.Duration;
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
        {
          @Override
          public Duration getRetryMinDuration()
          {
            return new Duration("PT1S");
          }

          @Override
          public Duration getRetryMaxDuration()
          {
            return new Duration("PT10S");
          }

          @Override
          public long getMaxRetryCount()
          {
            return 10;
          }
        }
    );

    Assert.assertEquals(new Duration("PT1S"), retryPolicy.getAndIncrementRetryDelay());
    Assert.assertEquals(new Duration("PT2S"), retryPolicy.getAndIncrementRetryDelay());
    Assert.assertEquals(new Duration("PT4S"), retryPolicy.getAndIncrementRetryDelay());
    Assert.assertEquals(new Duration("PT8S"), retryPolicy.getAndIncrementRetryDelay());
    Assert.assertEquals(new Duration("PT10S"), retryPolicy.getAndIncrementRetryDelay());
    Assert.assertEquals(new Duration("PT10S"), retryPolicy.getAndIncrementRetryDelay());
  }
}
