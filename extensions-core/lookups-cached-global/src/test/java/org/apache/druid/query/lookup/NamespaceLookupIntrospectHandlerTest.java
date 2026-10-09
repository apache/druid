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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.server.lookup.namespace.NamespaceExtractionConfig;
import org.apache.druid.server.lookup.namespace.cache.CacheHandler;
import org.apache.druid.server.lookup.namespace.cache.OffHeapNamespaceExtractionCacheManager;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class NamespaceLookupIntrospectHandlerTest
{
  private final NamespaceLookupExtractorFactory factory = Mockito.mock(NamespaceLookupExtractorFactory.class);
  private final NamespaceLookupIntrospectHandler handler = new NamespaceLookupIntrospectHandler(factory);
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @ParameterizedTest
  @ValueSource(strings = {"keys", "values", "map"})
  public void testResponseSurvivesOffHeapCacheDisposal(final String endpoint) throws Exception
  {
    final Map<String, String> expected = ImmutableMap.of("foo", "shared", "bar", "shared");
    final AtomicInteger releases = new AtomicInteger();
    final Lifecycle lifecycle = new Lifecycle();
    final OffHeapNamespaceExtractionCacheManager manager = new OffHeapNamespaceExtractionCacheManager(
        lifecycle,
        new NoopServiceEmitter(),
        new NamespaceExtractionConfig()
    );
    final Response response;
    lifecycle.start();
    try {
      final CacheHandler cache = manager.createCache();
      cache.getCache().putAll(expected);
      final RetainedLookupExtractor extractor = RetainedLookupExtractor.create(
          cache.asLookupExtractor(false, () -> new byte[0]),
          () -> {
            // Simulate a retired cache whose last reference is released by the handler.
            cache.close();
            releases.incrementAndGet();
          }
      );
      Mockito.when(factory.acquireRetainedLookupExtractor()).thenReturn(Optional.of(extractor));

      response = getResponse(endpoint);
      Assertions.assertEquals(1, releases.get());
      Mockito.verify(factory).acquireRetainedLookupExtractor();
      Mockito.verifyNoMoreInteractions(factory);
    }
    finally {
      lifecycle.stop();
    }

    // JAX-RS serializes the entity after the handler returns and its retained reference has been released.
    final Class<?> responseType = switch (endpoint) {
      case "keys" -> Set.class;
      case "values" -> List.class;
      default -> Map.class;
    };
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(
        expectedEntity(endpoint, expected),
        mapper.readValue(mapper.writeValueAsString(response.getEntity()), responseType)
    );
  }

  @ParameterizedTest
  @ValueSource(strings = {"keys", "values", "map"})
  public void testSnapshotPreservesNulls(final String endpoint)
  {
    final Map<String, String> expected = new LinkedHashMap<>();
    expected.put("empty", null);
    expected.put(null, "null-key");
    final Map<String, String> backingMap = new LinkedHashMap<>(expected);
    final RetainedLookupExtractor extractor = RetainedLookupExtractor.create(
        new MapLookupExtractor(backingMap, false),
        backingMap::clear
    );
    Mockito.when(factory.acquireRetainedLookupExtractor()).thenReturn(Optional.of(extractor));

    final Response response = getResponse(endpoint);
    Assertions.assertTrue(backingMap.isEmpty());
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(expectedEntity(endpoint, expected), response.getEntity());
  }

  @ParameterizedTest
  @ValueSource(strings = {"keys", "values", "map"})
  public void testReleasesReferenceWhenCopyFails(final String endpoint)
  {
    final RuntimeException failure = new RuntimeException("copy failed");
    final AtomicInteger releases = new AtomicInteger();
    final Map<String, String> failingMap = new AbstractMap<>()
    {
      @Override
      public Set<Entry<String, String>> entrySet()
      {
        throw failure;
      }
    };
    final RetainedLookupExtractor extractor = RetainedLookupExtractor.create(
        new MapLookupExtractor(failingMap, false),
        releases::incrementAndGet
    );
    Mockito.when(factory.acquireRetainedLookupExtractor()).thenReturn(Optional.of(extractor));

    Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, () -> getResponse(endpoint)));
    Assertions.assertEquals(1, releases.get());
  }

  @ParameterizedTest
  @ValueSource(strings = {"keys", "values", "map"})
  public void testUnavailableCacheReturnsNotFound(final String endpoint)
  {
    Mockito.when(factory.acquireRetainedLookupExtractor()).thenThrow(new ISE("cache unavailable"));
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), getResponse(endpoint).getStatus());
  }

  private Response getResponse(final String endpoint)
  {
    return switch (endpoint) {
      case "keys" -> handler.getKeys();
      case "values" -> handler.getValues();
      case "map" -> handler.getMap();
      default -> throw new AssertionError(endpoint);
    };
  }

  private Object expectedEntity(final String endpoint, final Map<String, String> map)
  {
    return switch (endpoint) {
      case "keys" -> map.keySet();
      case "values" -> new ArrayList<>(map.values());
      case "map" -> map;
      default -> throw new AssertionError(endpoint);
    };
  }
}
