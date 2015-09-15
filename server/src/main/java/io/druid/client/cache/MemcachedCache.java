/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.client.cache;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.metrics.AbstractMonitor;
import com.metamx.metrics.MonitorScheduler;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.metrics.MetricCollector;
import net.spy.memcached.metrics.MetricType;
import net.spy.memcached.ops.LinkedOperationQueueFactory;
import net.spy.memcached.ops.OperationQueueFactory;
import org.apache.commons.codec.digest.DigestUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class MemcachedCache implements Cache
{
  private static final Logger log = new Logger(MemcachedCache.class);

  final static HashAlgorithm MURMUR3_128 = new HashAlgorithm()
  {
    final HashFunction fn = Hashing.murmur3_128();

    @Override
    public long hash(String k)
    {
      return fn.hashString(k, Charsets.UTF_8).asLong();
    }

    @Override
    public String toString()
    {
      return fn.toString();
    }
  };

  public static MemcachedCache create(final MemcachedCacheConfig config, MonitorScheduler emitter)
  {
    final ConcurrentMap<String, AtomicLong> counters = new ConcurrentHashMap<>();
    final ConcurrentMap<String, AtomicLong> meters = new ConcurrentHashMap<>();
    emitter.addMonitor(
        new AbstractMonitor()
        {
          final AtomicReference<Map<String, Long>> priorValues = new AtomicReference<Map<String, Long>>(new HashMap<String, Long>());

          @Override
          public boolean doMonitor(ServiceEmitter emitter)
          {
            final Map<String, Long> priorValues = this.priorValues.get();
            final Map<String, Long> currentValues = getCurrentValues();
            final ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
            for (Map.Entry<String, Long> entry : currentValues.entrySet()) {
              emitter.emit(
                  builder.setDimension("memcached metric", entry.getKey())
                         .build("query/cache/memcached/total", entry.getValue())
              );
              final Long prior = priorValues.get(entry.getKey());
              if (prior != null) {
                emitter.emit(
                    builder.setDimension("memcached metric", entry.getKey()).build(
                        "query/cache/memcached/delta",
                        entry.getValue() - prior
                    )
                );
              }
            }

            if (!this.priorValues.compareAndSet(priorValues, currentValues)) {
              log.error("Prior value changed while I was reporting! updating anyways");
              this.priorValues.set(currentValues);
            }
            return true;
          }

          private Map<String, Long> getCurrentValues()
          {
            final ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();
            for (Map.Entry<String, AtomicLong> entry : counters.entrySet()) {
              builder.put(entry.getKey(), entry.getValue().get());
            }
            return builder.build();
          }
        }
    );
    try {
      LZ4Transcoder transcoder = new LZ4Transcoder(config.getMaxObjectSize());

      // always use compression
      transcoder.setCompressionThreshold(0);

      OperationQueueFactory opQueueFactory;
      long maxQueueBytes = config.getMaxOperationQueueSize();
      if (maxQueueBytes > 0) {
        opQueueFactory = new MemcachedOperationQueueFactory(maxQueueBytes);
      } else {
        opQueueFactory = new LinkedOperationQueueFactory();
      }

      final Predicate<String> interesting = new Predicate<String>()
      {
        @Override
        public boolean apply(@Nullable String input)
        {
          // See net.spy.memcached.MemcachedConnection.registerMetrics()
          // in current version shutdown queue metric is borked
          return input != null && !input.contains("Down");
        }
      };

      return new MemcachedCache(
          new MemcachedClient(
              new MemcachedCustomConnectionFactoryBuilder()
                  // 1000 repetitions gives us good distribution with murmur3_128
                  // (approx < 5% difference in counts across nodes, with 5 cache nodes)
                  .setKetamaNodeRepetitions(1000)
                  .setHashAlg(MURMUR3_128)
                  .setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                  .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT)
                  .setDaemon(true)
                  .setFailureMode(FailureMode.Cancel)
                  .setTranscoder(transcoder)
                  .setShouldOptimize(true)
                  .setOpQueueMaxBlockTime(config.getTimeout())
                  .setOpTimeout(config.getTimeout())
                  .setReadBufferSize(config.getReadBufferSize())
                  .setOpQueueFactory(opQueueFactory)
                  .setEnableMetrics(MetricType.DEBUG) // Not as scary as it sounds
                  .setWriteOpQueueFactory(opQueueFactory)
                  .setReadOpQueueFactory(opQueueFactory)
                  .setMetricCollector(
                      new MetricCollector()
                      {
                        @Override
                        public void addCounter(String name)
                        {
                          if (!interesting.apply(name)) {
                            return;
                          }
                          counters.put(name, new AtomicLong(0L));

                          if (log.isDebugEnabled()) {
                            log.debug("Add Counter [%s]", name);
                          }
                        }

                        @Override
                        public void removeCounter(String name)
                        {
                          if (!interesting.apply(name)) {
                            return;
                          }
                          counters.remove(name);

                          if (log.isDebugEnabled()) {
                            log.debug("Remove Counter [%s]", name);
                          }
                        }

                        @Override
                        public void incrementCounter(String name)
                        {
                          if (!interesting.apply(name)) {
                            return;
                          }
                          AtomicLong counter = counters.get(name);
                          if (counter == null) {
                            counters.putIfAbsent(name, new AtomicLong(0));
                            counter = counters.get(name);
                          }
                          counter.incrementAndGet();

                          if (log.isDebugEnabled()) {
                            log.debug("Incrament [%s]", name);
                          }
                        }

                        @Override
                        public void incrementCounter(String name, int amount)
                        {
                          if (!interesting.apply(name)) {
                            return;
                          }
                          AtomicLong counter = counters.get(name);
                          if (counter == null) {
                            counters.putIfAbsent(name, new AtomicLong(0));
                            counter = counters.get(name);
                          }
                          counter.addAndGet(amount);

                          if (log.isDebugEnabled()) {
                            log.debug("Increment [%s] %d", name, amount);
                          }
                        }

                        @Override
                        public void decrementCounter(String name)
                        {
                          if (!interesting.apply(name)) {
                            return;
                          }
                          AtomicLong counter = counters.get(name);
                          if (counter == null) {
                            counters.putIfAbsent(name, new AtomicLong(0));
                            counter = counters.get(name);
                          }
                          counter.decrementAndGet();

                          if (log.isDebugEnabled()) {
                            log.debug("Decrement [%s]", name);
                          }
                        }

                        @Override
                        public void decrementCounter(String name, int amount)
                        {
                          if (!interesting.apply(name)) {
                            return;
                          }
                          AtomicLong counter = counters.get(name);
                          if (counter == null) {
                            counters.putIfAbsent(name, new AtomicLong(0L));
                            counter = counters.get(name);
                          }
                          counter.addAndGet(-amount);

                          if (log.isDebugEnabled()) {
                            log.debug("Decrement [%s] %d", name, amount);
                          }
                        }

                        @Override
                        public void addMeter(String name)
                        {
                          meters.put(name, new AtomicLong(0L));
                          if (log.isDebugEnabled()) {
                            log.debug("Adding meter [%s]", name);
                          }
                        }

                        @Override
                        public void removeMeter(String name)
                        {
                          meters.remove(name);
                          if (log.isDebugEnabled()) {
                            log.debug("Removing meter [%s]", name);
                          }
                        }

                        @Override
                        public void markMeter(String name)
                        {
                          AtomicLong meter = meters.get(name);
                          if (meter == null) {
                            meters.putIfAbsent(name, new AtomicLong(0L));
                            meter = meters.get(name);
                          }
                          meter.incrementAndGet();

                          if (log.isDebugEnabled()) {
                            log.debug("Increment counter [%s]", name);
                          }
                        }

                        @Override
                        public void addHistogram(String name)
                        {
                          log.debug("Ignoring add histogram [%s]", name);
                        }

                        @Override
                        public void removeHistogram(String name)
                        {
                          log.debug("Ignoring remove histogram [%s]", name);
                        }

                        @Override
                        public void updateHistogram(String name, int amount)
                        {
                          log.debug("Ignoring update histogram [%s]: %d", name, amount);
                        }
                      }
                  )
                  .build(),
              AddrUtil.getAddresses(config.getHosts())
          ),
          config
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private final int timeout;
  private final int expiration;
  private final String memcachedPrefix;

  private final MemcachedClientIF client;

  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong missCount = new AtomicLong(0);
  private final AtomicLong timeoutCount = new AtomicLong(0);
  private final AtomicLong errorCount = new AtomicLong(0);


  MemcachedCache(MemcachedClientIF client, MemcachedCacheConfig config)
  {
    Preconditions.checkArgument(
        config.getMemcachedPrefix().length() <= MAX_PREFIX_LENGTH,
        "memcachedPrefix length [%d] exceeds maximum length [%d]",
        config.getMemcachedPrefix().length(),
        MAX_PREFIX_LENGTH
    );
    this.timeout = config.getTimeout();
    this.expiration = config.getExpiration();
    this.client = client;
    this.memcachedPrefix = config.getMemcachedPrefix();
  }

  @Override
  public CacheStats getStats()
  {
    return new CacheStats(
        hitCount.get(),
        missCount.get(),
        0,
        0,
        0,
        timeoutCount.get(),
        errorCount.get()
    );
  }

  @Override
  public byte[] get(NamedKey key)
  {
    Future<Object> future;
    try {
      future = client.asyncGet(computeKeyHash(memcachedPrefix, key));
    }
    catch (IllegalStateException e) {
      // operation did not get queued in time (queue is full)
      errorCount.incrementAndGet();
      log.warn(e, "Unable to queue cache operation");
      return null;
    }
    try {
      byte[] bytes = (byte[]) future.get(timeout, TimeUnit.MILLISECONDS);
      if (bytes != null) {
        hitCount.incrementAndGet();
      } else {
        missCount.incrementAndGet();
      }
      return bytes == null ? null : deserializeValue(key, bytes);
    }
    catch (TimeoutException e) {
      timeoutCount.incrementAndGet();
      future.cancel(false);
      return null;
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    catch (ExecutionException e) {
      errorCount.incrementAndGet();
      log.warn(e, "Exception pulling item from cache");
      return null;
    }
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    try {
      client.set(computeKeyHash(memcachedPrefix, key), expiration, serializeValue(key, value));
    }
    catch (IllegalStateException e) {
      // operation did not get queued in time (queue is full)
      errorCount.incrementAndGet();
      log.warn(e, "Unable to queue cache operation");
    }
  }

  private static byte[] serializeValue(NamedKey key, byte[] value)
  {
    byte[] keyBytes = key.toByteArray();
    return ByteBuffer.allocate(Ints.BYTES + keyBytes.length + value.length)
                     .putInt(keyBytes.length)
                     .put(keyBytes)
                     .put(value)
                     .array();
  }

  private static byte[] deserializeValue(NamedKey key, byte[] bytes)
  {
    ByteBuffer buf = ByteBuffer.wrap(bytes);

    final int keyLength = buf.getInt();
    byte[] keyBytes = new byte[keyLength];
    buf.get(keyBytes);
    byte[] value = new byte[buf.remaining()];
    buf.get(value);

    Preconditions.checkState(
        Arrays.equals(keyBytes, key.toByteArray()),
        "Keys do not match, possible hash collision?"
    );
    return value;
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    Map<String, NamedKey> keyLookup = Maps.uniqueIndex(
        keys,
        new Function<NamedKey, String>()
        {
          @Override
          public String apply(
              @Nullable NamedKey input
          )
          {
            return computeKeyHash(memcachedPrefix, input);
          }
        }
    );

    Map<NamedKey, byte[]> results = Maps.newHashMap();

    BulkFuture<Map<String, Object>> future;
    try {
      future = client.asyncGetBulk(keyLookup.keySet());
    }
    catch (IllegalStateException e) {
      // operation did not get queued in time (queue is full)
      errorCount.incrementAndGet();
      log.warn(e, "Unable to queue cache operation");
      return results;
    }

    try {
      Map<String, Object> some = future.getSome(timeout, TimeUnit.MILLISECONDS);

      if (future.isTimeout()) {
        future.cancel(false);
        timeoutCount.incrementAndGet();
      }
      missCount.addAndGet(keyLookup.size() - some.size());
      hitCount.addAndGet(some.size());

      for (Map.Entry<String, Object> entry : some.entrySet()) {
        final NamedKey key = keyLookup.get(entry.getKey());
        final byte[] value = (byte[]) entry.getValue();
        results.put(
            key,
            value == null ? null : deserializeValue(key, value)
        );
      }

      return results;
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    catch (ExecutionException e) {
      errorCount.incrementAndGet();
      log.warn(e, "Exception pulling item from cache");
      return results;
    }
  }

  @Override
  public void close(String namespace)
  {
    // no resources to cleanup
  }

  public static final int MAX_PREFIX_LENGTH =
      MemcachedClientIF.MAX_KEY_LENGTH
      - 40 // length of namespace hash
      - 40 // length of key hash
      - 2  // length of separators
      ;

  private static String computeKeyHash(String memcachedPrefix, NamedKey key)
  {
    // hash keys to keep things under 250 characters for memcached
    return memcachedPrefix + ":" + DigestUtils.sha1Hex(key.namespace) + ":" + DigestUtils.sha1Hex(key.key);
  }

  public boolean isLocal()
  {
    return false;
  }
}
