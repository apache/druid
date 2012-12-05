package com.metamx.druid.client.cache;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MemcachedCacheBrokerBenchmark extends SimpleBenchmark
{
  private static final String BASE_KEY = "test_2012-11-26T00:00:00.000Z_2012-11-27T00:00:00.000Z_2012-11-27T04:11:25.979Z_";

  private MemcachedCacheBroker broker;
  private Cache cache;
  private static byte[] randBytes;

  // object size in kB
  @Param({"1", "5", "10", "40"}) int objectSize;
  @Param({"100", "1000"}) int objectCount;

  @Override
  protected void setUp() throws Exception
  {
    broker = MemcachedCacheBroker.create(
        new MemcachedCacheBrokerConfig()
        {
          @Override
          public int getExpiration()
          {
            // 1 year
            return 3600 * 24 * 365;
          }

          @Override
          public int getTimeout()
          {
            // 500 milliseconds
            return 500;
          }

          @Override
          public String getHosts()
          {
            return "localhost:11211";
          }

          @Override
          public int getMaxObjectSize()
          {
            // 50 MB
            return 50 * 1024 * 1024;
          }
        }
    );

    cache = broker.provideCache("default");


    randBytes = new byte[objectSize * 1024];
    new Random(0).nextBytes(randBytes);
  }

  @Override
  protected void tearDown() throws Exception
  {
    broker.getClient().flush();
    broker.getClient().shutdown();
  }

  public void timePutObjects(int reps) {
    for(int i = 0; i < reps; ++i) {
      for(int k = 0; k < objectCount; ++k) {
        String key = BASE_KEY + i;
        cache.put(key.getBytes(), randBytes);
      }
      broker.getClient().waitForQueues(1, TimeUnit.HOURS);
    }
  }

  public byte[] timeGetObject(int reps) {
    byte[] bytes = null;
    for (int i = 0; i < reps; i++) {
      for(int k = 0; k < objectCount; ++k) {
        String key = BASE_KEY + i;
        bytes = cache.get(key.getBytes());
      }
    }
    return bytes;
  }

  public static void main(String[] args) throws Exception {
    Runner.main(MemcachedCacheBrokerBenchmark.class, args);
  }
}
