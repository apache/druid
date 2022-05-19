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

package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.asynchttpclient.AsyncHttpClient;

import java.io.Closeable;
import java.io.Flushable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class ParametrizedUriEmitter implements Flushable, Closeable, Emitter
{
  private static final Logger log = new Logger(ParametrizedUriEmitter.class);
  private static final Set<String> ONLY_FEED_PARAM = ImmutableSet.of("feed");

  private static UriExtractor makeUriExtractor(ParametrizedUriEmitterConfig config)
  {
    final String baseUri = config.getRecipientBaseUrlPattern();
    final ParametrizedUriExtractor parametrizedUriExtractor = new ParametrizedUriExtractor(baseUri);
    UriExtractor uriExtractor = parametrizedUriExtractor;
    if (ONLY_FEED_PARAM.equals(parametrizedUriExtractor.getParams())) {
      uriExtractor = new FeedUriExtractor(StringUtils.replace(baseUri, "{feed}", "%s"));
    }
    return uriExtractor;
  }

  /**
   * Type should be ConcurrentHashMap, not {@link java.util.concurrent.ConcurrentMap}, because the latter _doesn't_
   * guarantee that the lambda passed to {@link java.util.Map#computeIfAbsent} is executed at most once.
   */
  private final ConcurrentHashMap<URI, HttpPostEmitter> emitters = new ConcurrentHashMap<>();
  private final UriExtractor uriExtractor;
  private final Object startCloseLock = new Object();
  @GuardedBy("startCloseLock")
  private boolean started = false;
  @GuardedBy("startCloseLock")
  private boolean closed = false;
  private final Lifecycle innerLifecycle = new Lifecycle();
  private final AsyncHttpClient client;
  private final ObjectMapper jsonMapper;
  private final ParametrizedUriEmitterConfig config;

  public ParametrizedUriEmitter(
      ParametrizedUriEmitterConfig config,
      AsyncHttpClient client,
      ObjectMapper jsonMapper
  )
  {
    this(config, client, jsonMapper, makeUriExtractor(config));
  }

  public ParametrizedUriEmitter(
      ParametrizedUriEmitterConfig config,
      AsyncHttpClient client,
      ObjectMapper jsonMapper,
      UriExtractor uriExtractor
  )
  {
    this.config = config;
    this.client = client;
    this.jsonMapper = jsonMapper;
    this.uriExtractor = uriExtractor;
  }

  @Override
  @LifecycleStart
  public void start()
  {
    // Use full synchronized instead of atomic flag, because otherwise some thread may think that the emitter is already
    // started while it's in the process of starting by another thread.
    synchronized (startCloseLock) {
      if (started) {
        return;
      }
      started = true;
      try {
        innerLifecycle.start();
      }
      catch (RuntimeException e) {
        throw e;
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void emit(Event event)
  {
    try {
      URI uri = uriExtractor.apply(event);
      // get() before computeIfAbsent() is an optimization to avoid locking in computeIfAbsent() if not needed.
      // See https://github.com/apache/druid/pull/6898#discussion_r251384586.
      HttpPostEmitter emitter = emitters.get(uri);
      if (emitter == null) {
        try {
          emitter = emitters.computeIfAbsent(uri, u -> {
            try {
              return innerLifecycle.addMaybeStartManagedInstance(
                  new HttpPostEmitter(config.buildHttpEmitterConfig(u.toString()), client, jsonMapper)
              );
            }
            catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
        }
        catch (RuntimeException e) {
          log.error(e, "Error while creating or starting an HttpPostEmitter for URI[%s]", uri);
          return;
        }
      }
      emitter.emit(event);
    }
    catch (URISyntaxException e) {
      log.error(e, "Failed to extract URI for event[%s]", event.toMap());
    }
  }

  @Override
  @LifecycleStop
  public void close()
  {
    // Use full synchronized instead of atomic flag, because otherwise some thread may think that the emitter is already
    // closed while it's in the process of closing by another thread.
    synchronized (startCloseLock) {
      if (closed) {
        return;
      }
      closed = true;
      innerLifecycle.stop();
    }
  }

  @Override
  public void flush()
  {
    Exception thrown = null;
    for (HttpPostEmitter httpPostEmitter : emitters.values()) {
      try {
        httpPostEmitter.flush();
      }
      catch (Exception e) {
        // If flush was interrupted, exit the loop
        if (Thread.currentThread().isInterrupted()) {
          if (thrown != null) {
            e.addSuppressed(thrown);
          }
          throw new RuntimeException(e);
        }
        if (thrown == null) {
          thrown = e;
        } else {
          if (thrown != e) {
            thrown.addSuppressed(e);
          }
        }
      }
    }
    if (thrown != null) {
      throw new RuntimeException(thrown);
    }
  }

  public void forEachEmitter(BiConsumer<URI, HttpPostEmitter> action)
  {
    emitters.forEach(action);
  }

  @Override
  public String toString()
  {
    return "ParametrizedUriEmitter{" +
           "emitters=" + emitters.keySet() +
           ", uriExtractor=" + uriExtractor +
           ", config=" + config +
           '}';
  }
}
