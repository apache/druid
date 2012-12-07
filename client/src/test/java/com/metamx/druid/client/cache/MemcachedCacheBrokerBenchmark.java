package com.metamx.druid.client.cache;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;
import net.spy.memcached.transcoders.SerializingTranscoder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MemcachedCacheBrokerBenchmark extends SimpleBenchmark
{
  private static final String BASE_KEY = "test_2012-11-26T00:00:00.000Z_2012-11-27T00:00:00.000Z_2012-11-27T04:11:25.979Z_";

  private MemcachedCacheBroker broker;
  private MemcachedClientIF client;

  private Cache cache;
  private static byte[] randBytes;

  // object size in kB
  @Param({"1", "5", "10", "40"}) int objectSize;
  @Param({"100", "1000"}) int objectCount;

  @Override
  protected void setUp() throws Exception
  {
    SerializingTranscoder transcoder = new SerializingTranscoder(
        50 * 1024 * 1024 // 50 MB
    );
    // disable compression
    transcoder.setCompressionThreshold(Integer.MAX_VALUE);

    client = new MemcachedClient(
        new ConnectionFactoryBuilder().setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                                      .setHashAlg(DefaultHashAlgorithm.FNV1A_64_HASH)
                                      .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT)
                                      .setDaemon(true)
                                      .setFailureMode(FailureMode.Retry)
                                      .setTranscoder(transcoder)
                                      .setShouldOptimize(true)
                                      .build(),
        AddrUtil.getAddresses("localhost:11211")
    );

    broker = new MemcachedCacheBroker(
        client,
        500, // 500 milliseconds
        3600 * 24 * 365 // 1 year
    );

    cache = broker.provideCache("default");


    randBytes = new byte[objectSize * 1024];
    new Random(0).nextBytes(randBytes);
  }

  @Override
  protected void tearDown() throws Exception
  {
    client.flush();
    client.shutdown();
  }

  public void timePutObjects(int reps) {
    for(int i = 0; i < reps; ++i) {
      for(int k = 0; k < objectCount; ++k) {
        String key = BASE_KEY + i;
        cache.put(key.getBytes(), randBytes);
      }
      // make sure the write queue is empty
      client.waitForQueues(1, TimeUnit.HOURS);
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
