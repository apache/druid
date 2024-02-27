/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.client.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import net.spy.memcached.BroadcastOpFactory;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.CachedData;
import net.spy.memcached.ClientMode;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.MemcachedClientIF;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.BulkGetCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;
import org.apache.druid.collections.StupidResourceHolder;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.SocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 */
public class MemcachedCacheTest
{
  private static final Logger log = new Logger(MemcachedCacheTest.class);
  private static final byte[] HI = StringUtils.toUtf8("hiiiiiiiiiiiiiiiiiii");
  private static final byte[] HO = StringUtils.toUtf8("hooooooooooooooooooo");
  protected static final AbstractMonitor NOOP_MONITOR = new AbstractMonitor()
  {
    @Override
    public boolean doMonitor(ServiceEmitter emitter)
    {
      return false;
    }
  };
  private MemcachedCache cache;
  private final MemcachedCacheConfig memcachedCacheConfig = new MemcachedCacheConfig()
  {
    @Override
    public String getMemcachedPrefix()
    {
      return "druid-memcached-test";
    }

    @Override
    public int getTimeout()
    {
      return 10;
    }

    @Override
    public int getExpiration()
    {
      return 3600;
    }

    @Override
    public String getHosts()
    {
      return "localhost:9999";
    }
  };

  @Before
  public void setUp()
  {
    cache = new MemcachedCache(
        Suppliers.ofInstance(
            StupidResourceHolder.create(new MockMemcachedClient())
        ),
        memcachedCacheConfig,
        NOOP_MONITOR
    );
  }

  @Test
  public void testBasicInjection() throws Exception
  {
    final MemcachedCacheConfig config = new MemcachedCacheConfig()
    {
      @Override
      public String getHosts()
      {
        return "127.0.0.1:22";
      }
    };
    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/memcached");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
                binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);

                binder.bind(MemcachedCacheConfig.class).toInstance(config);
                binder.bind(Cache.class).toProvider(MemcachedProviderWithConfig.class).in(ManageLifecycle.class);
              }
            }
        )
    );
    Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
    lifecycle.start();
    try {
      Cache cache = injector.getInstance(Cache.class);
      Assert.assertEquals(MemcachedCache.class, cache.getClass());
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testSimpleInjection()
  {
    final String uuid = UUID.randomUUID().toString();
    System.setProperty(uuid + ".type", "memcached");
    System.setProperty(uuid + ".hosts", "localhost");
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/memcached");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
                binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);

                binder.bind(Cache.class).toProvider(CacheProvider.class);
                JsonConfigProvider.bind(binder, uuid, CacheProvider.class);
              }
            }
        )
    );
    final CacheProvider memcachedCacheProvider = injector.getInstance(CacheProvider.class);
    Assert.assertNotNull(memcachedCacheProvider);
    Assert.assertEquals(MemcachedCacheProvider.class, memcachedCacheProvider.getClass());
  }

  @Test
  public void testMonitor() throws Exception
  {
    final MemcachedCache cache = MemcachedCache.create(memcachedCacheConfig);
    final StubServiceEmitter serviceEmitter = new StubServiceEmitter("service", "host");

    while (serviceEmitter.getEvents().isEmpty()) {
      Thread.sleep(memcachedCacheConfig.getTimeout());
      cache.doMonitor(serviceEmitter);
    }

    Assert.assertFalse(serviceEmitter.getEvents().isEmpty());
    ObjectMapper mapper = new DefaultObjectMapper();
    for (Event event : serviceEmitter.getEvents()) {
      log.debug("Found event `%s`", mapper.writeValueAsString(event.toMap()));
    }
  }

  @Test
  public void testDefaultClientMode() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException
  {
    ConnectionFactory connectionFactory = MemcachedCache.createConnectionFactory(memcachedCacheConfig, null, null, null);
    // Ensure that clientMode is set to Static by default
    Assert.assertEquals(connectionFactory.getClientMode(), ClientMode.Static);
    Assert.assertNull(connectionFactory.getSSLContext());
  }
  @Test
  public void testConnectionFactory() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException
  {
    final MemcachedCacheConfig config = new MemcachedCacheConfig()
    {
      @Override
      public boolean enableTls()
      {
        return true;
      }

      @Override
      public String getClientMode()
      {
        return "dynamic";
      }
      @Override
      public String getHosts()
      {
        return "localhost:9999";
      }
    };
    // Dynamic mode
    ConnectionFactory connectionFactoryDynamic = MemcachedCache.createConnectionFactory(config, null, null, null);
    // Ensure client mode is set to the value passed in config.
    Assert.assertEquals(connectionFactoryDynamic.getClientMode(), ClientMode.Dynamic);
    //enableTls is true so sslContext is not null
    Assert.assertNotNull(connectionFactoryDynamic.getSSLContext());
  }

  @Test
  public void testInvalidClientMode()
  {
    final MemcachedCacheConfig config = new MemcachedCacheConfig()
    {

      @Override
      public String getClientMode()
      {
        return "invalid-name";
      }
    };
    RuntimeException exception = Assert.assertThrows(RuntimeException.class, () -> {
      MemcachedCache.createConnectionFactory(config, null, null, null);
    });
    Assert.assertEquals(exception.getMessage(), "Invalid value provided for `druid.cache.clientMode`. Value must be 'static' or 'dynamic'.");
  }
  @Test
  public void testSanity()
  {
    Assert.assertNull(cache.get(new Cache.NamedKey("a", HI)));
    put(cache, "a", HI, 1);
    Assert.assertEquals(1, get(cache, "a", HI));
    Assert.assertNull(cache.get(new Cache.NamedKey("the", HI)));

    put(cache, "the", HI, 2);
    Assert.assertEquals(1, get(cache, "a", HI));
    Assert.assertEquals(2, get(cache, "the", HI));

    put(cache, "the", HO, 10);
    Assert.assertEquals(1, get(cache, "a", HI));
    Assert.assertNull(cache.get(new Cache.NamedKey("a", HO)));
    Assert.assertEquals(2, get(cache, "the", HI));
    Assert.assertEquals(10, get(cache, "the", HO));

    cache.close("the");
    Assert.assertEquals(1, get(cache, "a", HI));
    Assert.assertNull(cache.get(new Cache.NamedKey("a", HO)));

    cache.close("a");
  }

  @Test
  public void testGetBulk()
  {
    Assert.assertNull(cache.get(new Cache.NamedKey("the", HI)));

    put(cache, "the", HI, 2);
    put(cache, "the", HO, 10);

    Cache.NamedKey key1 = new Cache.NamedKey("the", HI);
    Cache.NamedKey key2 = new Cache.NamedKey("the", HO);

    Map<Cache.NamedKey, byte[]> result = cache.getBulk(
        Lists.newArrayList(
            key1,
            key2
        )
    );

    Assert.assertEquals(2, Ints.fromByteArray(result.get(key1)));
    Assert.assertEquals(10, Ints.fromByteArray(result.get(key2)));
  }

  public void put(Cache cache, String namespace, byte[] key, Integer value)
  {
    cache.put(new Cache.NamedKey(namespace, key), Ints.toByteArray(value));
  }

  public int get(Cache cache, String namespace, byte[] key)
  {
    return Ints.fromByteArray(cache.get(new Cache.NamedKey(namespace, key)));
  }
}

class MemcachedProviderWithConfig extends MemcachedCacheProvider
{
  private final MemcachedCacheConfig config;

  @Inject
  public MemcachedProviderWithConfig(MemcachedCacheConfig config)
  {
    this.config = config;
  }

  @Override
  public Cache get()
  {
    return MemcachedCache.create(config);
  }
}

class MockMemcachedClient implements MemcachedClientIF
{
  private final ConcurrentMap<String, CachedData> theMap = new ConcurrentHashMap<String, CachedData>();
  private final SerializingTranscoder transcoder;

  public MockMemcachedClient()
  {
    transcoder = new LZ4Transcoder();
    transcoder.setCompressionThreshold(0);
  }

  @Override
  public Collection<SocketAddress> getAvailableServers()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean refreshCertificate()
  {
    return true;
  }

  @Override
  public Collection<SocketAddress> getUnavailableServers()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Transcoder<Object> getTranscoder()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public NodeLocator getNodeLocator()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<Boolean> append(long cas, String key, Object val)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<Boolean> append(String s, Object o)
  {
    return null;
  }

  @Override
  public <T> Future<Boolean> append(long cas, String key, T val, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> Future<Boolean> append(String s, T t, Transcoder<T> tTranscoder)
  {
    return null;
  }

  @Override
  public Future<Boolean> prepend(long cas, String key, Object val)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<Boolean> prepend(String s, Object o)
  {
    return null;
  }

  @Override
  public <T> Future<Boolean> prepend(long cas, String key, T val, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> Future<Boolean> prepend(String s, T t, Transcoder<T> tTranscoder)
  {
    return null;
  }

  @Override
  public <T> Future<CASResponse> asyncCAS(String key, long casId, T value, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<CASResponse> asyncCAS(String key, long casId, Object value)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<CASResponse> asyncCAS(String s, long l, int i, Object o)
  {
    return null;
  }

  @Override
  public <T> OperationFuture<CASResponse> asyncCAS(String s, long l, int i, T t, Transcoder<T> tTranscoder)
  {
    return null;
  }

  @Override
  public <T> CASResponse cas(String key, long casId, int exp, T value, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public CASResponse cas(String key, long casId, Object value)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public CASResponse cas(String s, long l, int i, Object o)
  {
    return null;
  }

  @Override
  public <T> CASResponse cas(String s, long l, T t, Transcoder<T> tTranscoder)
  {
    return null;
  }

  @Override
  public <T> Future<Boolean> add(String key, int exp, T o, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<Boolean> add(String key, int exp, Object o)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> Future<Boolean> set(String key, int exp, T o, Transcoder<T> tc)
  {
    theMap.put(key, tc.encode(o));

    return new Future<Boolean>()
    {
      @Override
      public boolean cancel(boolean b)
      {
        return false;
      }

      @Override
      public boolean isCancelled()
      {
        return false;
      }

      @Override
      public boolean isDone()
      {
        return true;
      }

      @Override
      public Boolean get()
      {
        return true;
      }

      @Override
      public Boolean get(long l, TimeUnit timeUnit)
      {
        return true;
      }
    };
  }

  @Override
  public Future<Boolean> set(String key, int exp, Object o)
  {
    return set(key, exp, o, transcoder);
  }

  @Override
  public <T> Future<Boolean> replace(String key, int exp, T o, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<Boolean> replace(String key, int exp, Object o)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> Future<T> asyncGet(String key, final Transcoder<T> tc)
  {
    CachedData data = theMap.get(key);
    final T theValue = data != null ? tc.decode(data) : null;

    return new Future<T>()
    {
      @Override
      public boolean cancel(boolean b)
      {
        return false;
      }

      @Override
      public boolean isCancelled()
      {
        return false;
      }

      @Override
      public boolean isDone()
      {
        return true;
      }

      @Override
      public T get()
      {
        return theValue;
      }

      @Override
      public T get(long l, TimeUnit timeUnit)
      {
        return theValue;
      }
    };
  }

  @Override
  public Future<Object> asyncGet(String key)
  {
    return asyncGet(key, transcoder);
  }

  @Override
  public Future<CASValue<Object>> asyncGetAndTouch(String key, int exp)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> Future<CASValue<T>> asyncGetAndTouch(String key, int exp, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public CASValue<Object> getAndTouch(String key, int exp)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> CASValue<T> getAndTouch(String key, int exp, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> Future<CASValue<T>> asyncGets(String key, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<CASValue<Object>> asyncGets(String key)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> CASValue<T> gets(String key, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public CASValue<Object> gets(String key)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> T get(String key, Transcoder<T> tc)
  {
    CachedData data = theMap.get(key);
    return data != null ? tc.decode(data) : null;
  }

  @Override
  public Object get(String key)
  {
    return get(key, transcoder);
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keys, Iterator<Transcoder<T>> tcs)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys, Iterator<Transcoder<T>> tcs)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(final Iterator<String> keys, final Transcoder<T> tc)
  {
    return new BulkFuture<Map<String, T>>()
    {
      @Override
      public boolean isTimeout()
      {
        return false;
      }

      @Override
      public Map<String, T> getSome(long timeout, TimeUnit unit)
      {
        return get();
      }

      @Override
      public OperationStatus getStatus()
      {
        return null;
      }

      @Override
      public Future<Map<String, T>> addListener(BulkGetCompletionListener bulkGetCompletionListener)
      {
        return null;
      }

      @Override
      public Future<Map<String, T>> removeListener(BulkGetCompletionListener bulkGetCompletionListener)
      {
        return null;
      }

      @Override
      public boolean cancel(boolean b)
      {
        return false;
      }

      @Override
      public boolean isCancelled()
      {
        return false;
      }

      @Override
      public boolean isDone()
      {
        return true;
      }

      @Override
      public Map<String, T> get()
      {
        Map<String, T> retVal = new HashMap<>();

        while (keys.hasNext()) {
          String key = keys.next();
          CachedData data = theMap.get(key);
          retVal.put(key, data != null ? tc.decode(data) : null);
        }

        return retVal;
      }

      @Override
      public Map<String, T> get(long l, TimeUnit timeUnit)
      {
        return get();
      }
    };
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(Iterator<String> keys)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(final Collection<String> keys)
  {
    return asyncGetBulk(keys.iterator(), transcoder);
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Transcoder<T> tc, String... keys)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(String... keys)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> Map<String, T> getBulk(Iterator<String> keys, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> Map<String, T> getBulk(Collection<String> keys, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Map<String, Object> getBulk(Iterator<String> keys)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Map<String, Object> getBulk(Collection<String> keys)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> Map<String, T> getBulk(Transcoder<T> tc, String... keys)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Map<String, Object> getBulk(String... keys)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> Future<Boolean> touch(String key, int exp, Transcoder<T> tc)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T> Future<Boolean> touch(String key, int exp)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Map<SocketAddress, String> getVersions()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Map<SocketAddress, Map<String, String>> getStats()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Map<SocketAddress, Map<String, String>> getStats(String prefix)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long incr(String key, long by)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long incr(String key, int by)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long decr(String key, long by)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long decr(String key, int by)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long incr(String key, long by, long def, int exp)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long incr(String key, int by, long def, int exp)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long decr(String key, long by, long def, int exp)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long decr(String key, int by, long def, int exp)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<Long> asyncIncr(String s, long l, long l2, int i)
  {
    return null;
  }

  @Override
  public Future<Long> asyncIncr(String s, int i, long l, int i2)
  {
    return null;
  }

  @Override
  public Future<Long> asyncDecr(String s, long l, long l2, int i)
  {
    return null;
  }

  @Override
  public Future<Long> asyncDecr(String s, int i, long l, int i2)
  {
    return null;
  }

  @Override
  public Future<Long> asyncIncr(String key, long by)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<Long> asyncIncr(String key, int by)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<Long> asyncDecr(String key, long by)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<Long> asyncDecr(String key, int by)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long incr(String key, long by, long def)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long incr(String key, int by, long def)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long decr(String key, long by, long def)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long decr(String key, int by, long def)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<Long> asyncIncr(String s, long l, long l2)
  {
    return null;
  }

  @Override
  public Future<Long> asyncIncr(String s, int i, long l)
  {
    return null;
  }

  @Override
  public Future<Long> asyncDecr(String s, long l, long l2)
  {
    return null;
  }

  @Override
  public Future<Long> asyncDecr(String s, int i, long l)
  {
    return null;
  }

  @Override
  public Future<Boolean> delete(String key)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<Boolean> delete(String s, long l)
  {
    return null;
  }

  @Override
  public Future<Boolean> flush(int delay)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Future<Boolean> flush()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void shutdown()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean shutdown(long timeout, TimeUnit unit)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean waitForQueues(long timeout, TimeUnit unit)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean addObserver(ConnectionObserver obs)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean removeObserver(ConnectionObserver obs)
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public CountDownLatch broadcastOp(BroadcastOpFactory broadcastOpFactory)
  {
    return null;
  }

  @Override
  public CountDownLatch broadcastOp(BroadcastOpFactory broadcastOpFactory, Collection<MemcachedNode> memcachedNodes)
  {
    return null;
  }

  @Override
  public Set<String> listSaslMechanisms()
  {
    throw new UnsupportedOperationException("not implemented");
  }
}
