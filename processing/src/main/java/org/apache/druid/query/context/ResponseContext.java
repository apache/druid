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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.BiFunction;

/**
 * The context for storing and passing data between chains of {@link org.apache.druid.query.QueryRunner}s.
 * The context is also transferred between Druid nodes with all the data it contains.
 */
@PublicApi
public abstract class ResponseContext
{
  /**
   * Keys associated with objects in the context.
   */
  public enum Key
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
     */
    QUERY_FAIL_TIME("queryFailTime"),
    /**
     * Query total bytes gathered.
     */
    QUERY_TOTAL_BYTES_GATHERED("queryTotalBytesGathered"),
    /**
     * This variable indicates when a running query should be expired,
     * and is effective only when 'timeout' of queryContext has a positive value.
     */
    TIMEOUT_AT("timeoutAt"),
    /**
     * The number of scanned rows.
     */
    COUNT(
        "count",
            (oldValue, newValue) -> (long) oldValue + (long) newValue
    ),
    /**
     * CPU consumed while processing a request.
     */
    CPU_CONSUMED(
        "cpuConsumed",
            (oldValue, newValue) -> (long) oldValue + (long) newValue
    );

    private final String name;
    /**
     * Merge function associated with a key: Object (Object oldValue, Object newValue)
     */
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

  public Object put(Key key, Object value)
  {
    return getDelegate().put(key.name, value);
  }

  public Object get(Key key)
  {
    return getDelegate().get(key.name);
  }

  public Object remove(Key key)
  {
    return getDelegate().remove(key.name);
  }

  /**
   * Merges a new value associated with a key with an old value.
   */
  public Object merge(Key key, Object value)
  {
    return getDelegate().merge(key.name, value, key.mergeFunction);
  }

  /**
   * Merges a response context into current.
   * This method merges only keys from the enum {@link Key}.
   */
  public void merge(ResponseContext responseContext)
  {
    for (Key key : Key.values()) {
      final Object newValue = responseContext.get(key);
      if (newValue != null) {
        merge(key, newValue);
      }
    }
  }

  /**
   * Serializes the context given that the resulting string length is less than the provided limit.
   * The method removes max-length fields one by one if the resulting string length is greater than the limit.
   * The resulting string might be correctly deserialized as a {@link ResponseContext}.
   */
  public SerializationResult serializeWith(ObjectMapper objectMapper, int maxLength) throws JsonProcessingException
  {
    final String fullSerializedString = objectMapper.writeValueAsString(getDelegate());
    if (fullSerializedString.length() <= maxLength) {
      return new SerializationResult(fullSerializedString, fullSerializedString, false);
    } else {
      final HashMap<String, Object> copiedMap = new HashMap<>(getDelegate());
      final PriorityQueue<Pair<String, String>> serializedPairs = new PriorityQueue<>((o1, o2) -> {
        Preconditions.checkNotNull(o1.rhs);
        Preconditions.checkNotNull(o2.rhs);
        return o2.rhs.length() - o1.rhs.length();
      });
      for (Map.Entry<String, Object> e : copiedMap.entrySet()) {
        serializedPairs.add(new Pair<>(e.getKey(), objectMapper.writeValueAsString(e.getValue())));
      }
      while (!copiedMap.isEmpty()) {
        final Pair<String, String> maxLengthPair = serializedPairs.poll();
        Preconditions.checkNotNull(maxLengthPair);
        copiedMap.remove(maxLengthPair.lhs);
        final String reducedSerializedString = objectMapper.writeValueAsString(copiedMap);
        if (reducedSerializedString.length() <= maxLength) {
          return new SerializationResult(reducedSerializedString, fullSerializedString, true);
        }
      }
      final String serializedEmptyMap = objectMapper.writeValueAsString(copiedMap);
      return new SerializationResult(serializedEmptyMap, fullSerializedString, true);
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
    private final String result;
    private final String fullResult;
    private final Boolean isReduced;

    SerializationResult(String result, String fullResult, Boolean isReduced)
    {
      this.result = result;
      this.fullResult = fullResult;
      this.isReduced = isReduced;
    }

    public String getResult()
    {
      return result;
    }

    public String getFullResult()
    {
      return fullResult;
    }

    public Boolean isReduced()
    {
      return isReduced;
    }
  }
}
