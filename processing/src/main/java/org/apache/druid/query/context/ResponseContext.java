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

package org.apache.druid.query.context;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.function.BiFunction;

/**
 * The context for storing and passing data between chains of {@link org.apache.druid.query.QueryRunner}s.
 * The context is also transferred between Druid nodes with all the data it contains.
 */
@PublicApi
public abstract class ResponseContext
{
  /**
   * The base interface of a response context key.
   * Should be implemented by every context key.
   */
  public interface BaseKey
  {
    String getName();
    /**
     * Merge function associated with a key: Object (Object oldValue, Object newValue)
     */
    BiFunction<Object, Object, Object> getMergeFunction();
  }

  /**
   * Keys associated with objects in the context. The enum is extension-friendly.
   * <p>If it's necessary to have some new keys in the context then they could be described in a separate enum:
   * <pre>{@code
   * public enum ExtensionResponseContextKey implements BaseKey
   * {
   *   EXTENSION_KEY_1("extension_key_1"), EXTENSION_KEY_2("extension_key_2");
   *
   *   static {
   *     for (ResponseContextKey key : values()) ResponseContext.Key.addKey(key);
   *   }
   *
   *   private final String name;
   *   private final BiFunction<Object, Object, Object> mergeFunction;
   *
   *   ExtensionResponseContextKey(String name)
   *   {
   *     this.name = name;
   *     this.mergeFunction = (oldValue, newValue) -> newValue;
   *   }
   *
   *   @Override public String getName() { return name; }
   *
   *   @Override public BiFunction<Object, Object, Object> getMergeFunction() { return mergeFunction; }
   * }
   * }</pre>
   * Make sure all extension enum values added with Key.addKey method.
   */
  public enum Key implements BaseKey
  {
    /**
     * Lists intervals for which NO segment is present.
     */
    UNCOVERED_INTERVALS(
        "uncoveredIntervals",
            (oldValue, newValue) -> {
              final ArrayList<Interval> result = new ArrayList<Interval>((List) oldValue);
              result.addAll((List) newValue);
              return result;
            }
    ),
    /**
     * Indicates if the number of uncovered intervals exceeded the limit (true/false).
     */
    UNCOVERED_INTERVALS_OVERFLOWED(
        "uncoveredIntervalsOverflowed",
            (oldValue, newValue) -> (boolean) oldValue || (boolean) newValue
    ),
    /**
     * Lists missing segments.
     */
    MISSING_SEGMENTS(
        "missingSegments",
            (oldValue, newValue) -> {
              final ArrayList<SegmentDescriptor> result = new ArrayList<SegmentDescriptor>((List) oldValue);
              result.addAll((List) newValue);
              return result;
            }
    ),
    /**
     * Entity tag. A part of HTTP cache validation mechanism.
     * Is being removed from the context before sending and used as a separate HTTP header.
     */
    ETAG("ETag"),
    /**
     * Query fail time (current time + timeout).
     * The final value in comparison to continuously updated TIMEOUT_AT.
     */
    QUERY_FAIL_DEADLINE_MILLIS("queryFailTime"),
    /**
     * Query total bytes gathered.
     */
    QUERY_TOTAL_BYTES_GATHERED("queryTotalBytesGathered"),
    /**
     * This variable indicates when a running query should be expired,
     * and is effective only when 'timeout' of queryContext has a positive value.
     * Continuously updated by {@link org.apache.druid.query.scan.ScanQueryEngine}
     * by reducing its value on the time of every scan iteration.
     */
    TIMEOUT_AT("timeoutAt"),
    /**
     * The number of scanned rows.
     * For backward compatibility the context key name still equals to "count".
     */
    NUM_SCANNED_ROWS(
        "count",
            (oldValue, newValue) -> (long) oldValue + (long) newValue
    ),
    /**
     * The total CPU time for threads related to Sequence processing of the query.
     * Resulting value on a Broker is a sum of downstream values from historicals / realtime nodes.
     * For additional information see {@link org.apache.druid.query.CPUTimeMetricQueryRunner}
     */
    CPU_CONSUMED_NANOS(
        "cpuConsumed",
            (oldValue, newValue) -> (long) oldValue + (long) newValue
    );

    /**
     * TreeMap is used to have the natural ordering of its keys
     */
    private static Map<String, BaseKey> map = new TreeMap<>();

    static {
      for (BaseKey key : values()) {
        addKey(key);
      }
    }

    /**
     * The primary way of registering context keys.
     * Only the keys registered this way are considered during the context merge.
     */
    public static void addKey(BaseKey key)
    {
      Preconditions.checkState(
          !map.containsKey(key.getName()),
          "ResponseContext keys already has the key with [%s] name",
          key.getName()
      );
      map.put(key.getName(), key);
    }

    /**
     * Returns a key associated with the name if the key was added via addKey method
     */
    public static BaseKey keyOf(String name)
    {
      return map.get(name);
    }

    /**
     * Returns all keys the enum contains and the added via addKey method
     */
    public static Collection<BaseKey> getKeys()
    {
      return map.values();
    }

    private final String name;

    private final BiFunction<Object, Object, Object> mergeFunction;

    Key(String name)
    {
      this.name = name;
      this.mergeFunction = (oldValue, newValue) -> newValue;
    }

    Key(String name, BiFunction<Object, Object, Object> mergeFunction)
    {
      this.name = name;
      this.mergeFunction = mergeFunction;
    }

    @Override
    public String getName()
    {
      return name;
    }

    @Override
    public BiFunction<Object, Object, Object> getMergeFunction()
    {
      return mergeFunction;
    }
  }

  /**
   * Create an empty DefaultResponseContext instance
   * @return empty DefaultResponseContext instance
   */
  public static ResponseContext createEmpty()
  {
    return DefaultResponseContext.createEmpty();
  }

  public static ResponseContext deserialize(String responseContext, ObjectMapper objectMapper) throws IOException
  {
    final Map<String, Object> delegate = objectMapper.readValue(
        responseContext,
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
    return new ResponseContext()
    {
      @Override
      protected Map<String, Object> getDelegate()
      {
        return delegate;
      }
    };
  }

  protected abstract Map<String, Object> getDelegate();

  public Object put(BaseKey key, Object value)
  {
    return getDelegate().put(key.getName(), value);
  }

  public Object get(BaseKey key)
  {
    return getDelegate().get(key.getName());
  }

  public Object remove(BaseKey key)
  {
    return getDelegate().remove(key.getName());
  }

  /**
   * Adds (merges) a new value associated with a key to an old value.
   * See merge function of a context key for a specific implementation.
   */
  public Object add(BaseKey key, Object value)
  {
    return getDelegate().merge(key.getName(), value, key.getMergeFunction());
  }

  /**
   * Merges a response context into current.
   * This method merges only keys from the enum {@link Key}.
   */
  public void merge(ResponseContext responseContext)
  {
    for (BaseKey key : Key.getKeys()) {
      final Object newValue = responseContext.get(key);
      if (newValue != null) {
        add(key, newValue);
      }
    }
  }

  /**
   * Serializes the context given that the resulting string length is less than the provided limit.
   * The method removes max-length fields one by one if the resulting string length is greater than the limit.
   * The resulting string might be correctly deserialized as a {@link ResponseContext}.
   */
  public SerializationResult serializeWith(ObjectMapper objectMapper, int maxCharsNumber) throws JsonProcessingException
  {
    final String fullSerializedString = objectMapper.writeValueAsString(getDelegate());
    if (fullSerializedString.length() <= maxCharsNumber) {
      return new SerializationResult(fullSerializedString, fullSerializedString);
    } else {
      final HashMap<String, Object> copiedMap = new HashMap<>(getDelegate());
      final PriorityQueue<Map.Entry<String, String>> serializedValueEntries = new PriorityQueue<>(
          Comparator.comparing((Map.Entry<String, String> e) -> e.getValue().length()).reversed()
      );
      for (Map.Entry<String, Object> e : copiedMap.entrySet()) {
        serializedValueEntries.add(new AbstractMap.SimpleImmutableEntry<>(
            e.getKey(),
            objectMapper.writeValueAsString(e.getValue())
        ));
      }
      // quadratic complexity: while loop with map serialization on each iteration
      while (!copiedMap.isEmpty() && !serializedValueEntries.isEmpty()) {
        final Map.Entry<String, String> maxLengthEntry = serializedValueEntries.poll();
        Preconditions.checkNotNull(maxLengthEntry);
        copiedMap.remove(maxLengthEntry.getKey());
        final String reducedSerializedString = objectMapper.writeValueAsString(copiedMap);
        if (reducedSerializedString.length() <= maxCharsNumber) {
          return new SerializationResult(reducedSerializedString, fullSerializedString);
        }
      }
      final String serializedEmptyMap = objectMapper.writeValueAsString(copiedMap);
      return new SerializationResult(serializedEmptyMap, fullSerializedString);
    }
  }

  /**
   * Serialization result of {@link ResponseContext}.
   * Response context might be serialized using max legth limit, in this case the context might be reduced
   * by removing max-length fields one by one unless serialization result length is less than the limit.
   * This structure has a reduced serialization result along with full result and boolean property
   * indicating if some fields were removed from the context.
   */
  public static class SerializationResult
  {
    private final String truncatedResult;
    private final String fullResult;

    SerializationResult(String truncatedResult, String fullResult)
    {
      this.truncatedResult = truncatedResult;
      this.fullResult = fullResult;
    }

    public String getTruncatedResult()
    {
      return truncatedResult;
    }

    public String getFullResult()
    {
      return fullResult;
    }

    public Boolean isReduced()
    {
      return !truncatedResult.equals(fullResult);
    }
  }
}
