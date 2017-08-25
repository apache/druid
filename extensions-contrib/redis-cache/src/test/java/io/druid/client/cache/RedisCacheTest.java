/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client.cache;

import com.fiftyonred.mock_jedis.MockJedis;
import com.fiftyonred.mock_jedis.MockJedisPool;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.ManageLifecycle;
import io.druid.initialization.Initialization;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.Lifecycle;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.UUID;

public class RedisCacheTest
{
    private static final byte[] HI = StringUtils.toUtf8("hiiiiiiiiiiiiiiiiiii");
    private static final byte[] HO = StringUtils.toUtf8("hooooooooooooooooooo");

    private RedisCache cache;
    private final RedisCacheConfig cacheConfig = new RedisCacheConfig()
    {
        @Override
        public int getTimeout()
        {
            return 10;
        }

        @Override
        public long getExpiration()
        {
            return 3600000;
        }
    };

    @Before
    public void setUp() throws Exception
    {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(cacheConfig.getMaxTotalConnections());
        poolConfig.setMaxIdle(cacheConfig.getMaxIdleConnections());
        poolConfig.setMinIdle(cacheConfig.getMinIdleConnections());

        MockJedisPool pool = new MockJedisPool(poolConfig, "localhost");
        // orginal MockJedis do not support 'milliseconds' in long type,
        // for test we override to support it
        pool.setClient(new MockJedis("localhost") {
            @Override
            public String psetex(byte[] key, long milliseconds, byte[] value)
            {
                return this.psetex(key, (int) milliseconds, value);
            }
        });

        cache = RedisCache.create(pool, cacheConfig);
    }

    @Test
    public void testBasicInjection() throws Exception
    {
        final RedisCacheConfig config = new RedisCacheConfig();
        Injector injector = Initialization.makeInjectorWithModules(
                GuiceInjectors.makeStartupInjector(), ImmutableList.of(
                        binder -> {
                            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/redis");
                            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
                            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);

                            binder.bind(RedisCacheConfig.class).toInstance(config);
                            binder.bind(Cache.class).toProvider(RedisCacheProviderWithConfig.class).in(ManageLifecycle.class);
                        }
                )
        );
        Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
        lifecycle.start();
        try {
            Cache cache = injector.getInstance(Cache.class);
            Assert.assertEquals(RedisCache.class, cache.getClass());
        }
        finally {
            lifecycle.stop();
        }
    }

    @Test
    public void testSimpleInjection()
    {
        final String uuid = UUID.randomUUID().toString();
        System.setProperty(uuid + ".type", "redis");
        final Injector injector = Initialization.makeInjectorWithModules(
                GuiceInjectors.makeStartupInjector(), ImmutableList.of(
                        binder -> {
                            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/redis");
                            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
                            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);

                            binder.bind(Cache.class).toProvider(CacheProvider.class);
                            JsonConfigProvider.bind(binder, uuid, CacheProvider.class);
                        }
                )
        );
        final CacheProvider cacheProvider = injector.getInstance(CacheProvider.class);
        Assert.assertNotNull(cacheProvider);
        Assert.assertEquals(RedisCacheProvider.class, cacheProvider.getClass());
    }

    @Test
    public void testSanity() throws Exception
    {
        Assert.assertNull(cache.get(new Cache.NamedKey("a", HI)));
        put(cache, "a", HI, 0);
        Assert.assertEquals(0, get(cache, "a", HI));
        Assert.assertNull(cache.get(new Cache.NamedKey("the", HI)));

        put(cache, "the", HI, 1);
        Assert.assertEquals(0, get(cache, "a", HI));
        Assert.assertEquals(1, get(cache, "the", HI));

        put(cache, "the", HO, 10);
        Assert.assertEquals(0, get(cache, "a", HI));
        Assert.assertNull(cache.get(new Cache.NamedKey("a", HO)));
        Assert.assertEquals(1, get(cache, "the", HI));
        Assert.assertEquals(10, get(cache, "the", HO));

        cache.close("the");
        Assert.assertEquals(0, get(cache, "a", HI));
        Assert.assertNull(cache.get(new Cache.NamedKey("a", HO)));
    }

    @Test
    public void testGetBulk() throws Exception
    {
        Assert.assertNull(cache.get(new Cache.NamedKey("the", HI)));

        put(cache, "the", HI, 1);
        put(cache, "the", HO, 10);

        Cache.NamedKey key1 = new Cache.NamedKey("the", HI);
        Cache.NamedKey key2 = new Cache.NamedKey("the", HO);
        Cache.NamedKey key3 = new Cache.NamedKey("a", HI);

        Map<Cache.NamedKey, byte[]> result = cache.getBulk(
                Lists.newArrayList(
                        key1,
                        key2,
                        key3
                )
        );

        Assert.assertEquals(1, Ints.fromByteArray(result.get(key1)));
        Assert.assertEquals(10, Ints.fromByteArray(result.get(key2)));
        Assert.assertEquals(null, result.get(key3));
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

class RedisCacheProviderWithConfig extends RedisCacheProvider
{
    private final RedisCacheConfig config;

    @Inject
    public RedisCacheProviderWithConfig(RedisCacheConfig config)
    {
        this.config = config;
    }

    @Override
    public Cache get()
    {
        return RedisCache.create(config);
    }
}

