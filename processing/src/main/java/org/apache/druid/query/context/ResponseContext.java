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

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
    @JsonValue
    String getName();
    /**
     * Merge function associated with a key: Object (Object oldValue, Object newValue)
     */
    BiFunction<Object, Object, Object> getMergeFunction();
  }

  /**
   * Keys associated with objects in the context.
   * <p>
   * If it's necessary to have some new keys in the context then they might be listed in a separate enum:
   * <pre>{@code
   * public enum ExtensionResponseContextKey implements BaseKey
   * {
   *   EXTENSION_KEY_1("extension_key_1"), EXTENSION_KEY_2("extension_key_2");
   *
   *   static {
   *     for (BaseKey key : values()) ResponseContext.Key.registerKey(key);
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
   * Make sure all extension enum values added with {@link Key#registerKey} method.
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
     * It is not updated continuously as {@link Key#TIMEOUT_AT}.
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
            (oldValue, newValue) -> ((Number) oldValue).longValue() + ((Number) newValue).longValue()
    ),
    /**
     * The total CPU time for threads related to Sequence processing of the query.
     * Resulting value on a Broker is a sum of downstream values from historicals / realtime nodes.
     * For additional information see {@link org.apache.druid.query.CPUTimeMetricQueryRunner}
     */
    CPU_CONSUMED_NANOS(
        "cpuConsumed",
            (oldValue, newValue) -> ((Number) oldValue).longValue() + ((Number) newValue).longValue()
    ),
    /**
     * Indicates if a {@link ResponseContext} was truncated during serialization.
     */
    TRUNCATED(
        "truncated",
            (oldValue, newValue) -> (boolean) oldValue || (boolean) newValue
    );

    /**
     * TreeMap is used to have the natural ordering of its keys
     */
    private static final Map<String, BaseKey> REGISTERED_KEYS = new TreeMap<>();

    static {
      for (BaseKey key : values()) {
        registerKey(key);
      }
    }

    /**
     * Primary way of registering context keys.
     * @throws IllegalArgumentException if the key has already been registered.
     */
    public static synchronized void registerKey(BaseKey key)
    {
      Preconditions.checkArgument(
          !REGISTERED_KEYS.containsKey(key.getName()),
          "Key [%s] has already been registered as a context key",
          key.getName()
      );
      REGISTERED_KEYS.put(key.getName(), key);
    }

    /**
     * Returns a registered key associated with the name {@param name}.
     * @throws IllegalStateException if a corresponding key has not been registered.
     */
    public static BaseKey keyOf(String name)
    {
      Preconditions.checkState(
           REGISTERED_KEYS.containsKey(name),
          "Key [%s] has not yet been registered as a context key",
          name
      );
      return REGISTERED_KEYS.get(name);
    }

    /**
     * Returns all keys registered via {@link Key#registerKey}.
     */
    public static Collection<BaseKey> getAllRegisteredKeys()
    {
      return Collections.unmodifiableCollection(REGISTERED_KEYS.values());
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

  protected abstract Map<BaseKey, Object> getDelegate();

  private static final Comparator<Map.Entry<String, JsonNode>> VALUE_LENGTH_REVERSED_COMPARATOR =
      Comparator.comparing((Map.Entry<String, JsonNode> e) -> e.getValue().toString().length()).reversed();

  /**
   * Create an empty DefaultResponseContext instance
   * @return empty DefaultResponseContext instance
   */
  public static ResponseContext createEmpty()
  {
    return DefaultResponseContext.createEmpty();
  }

  /**
   * Deserializes a string into {@link ResponseContext} using given {@link ObjectMapper}.
   * @throws IllegalStateException if one of the deserialized map keys has not been registered.
   */
  public static ResponseContext deserialize(String responseContext, ObjectMapper objectMapper) throws IOException
  {
    final Map<String, Object> keyNameToObjects = objectMapper.readValue(
        responseContext,
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
    final ResponseContext context = ResponseContext.createEmpty();
    keyNameToObjects.forEach((keyName, value) -> {
      final BaseKey key = Key.keyOf(keyName);
      context.add(key, value);
    });
    return context;
  }

  /**
   * Associates the specified object with the specified key.
   * @throws IllegalStateException if the key has not been registered.
   */
  public Object put(BaseKey key, Object value)
  {
    final BaseKey registeredKey = Key.keyOf(key.getName());
    return getDelegate().put(registeredKey, value);
  }

  public Object get(BaseKey key)
  {
    return getDelegate().get(key);
  }

  public Object remove(BaseKey key)
  {
    return getDelegate().remove(key);
  }

  /**
   * Adds (merges) a new value associated with a key to an old value.
   * See merge function of a context key for a specific implementation.
   * @throws IllegalStateException if the key has not been registered.
   */
  public Object add(BaseKey key, Object value)
  {
    final BaseKey registeredKey = Key.keyOf(key.getName());
    return getDelegate().merge(registeredKey, value, key.getMergeFunction());
  }

  /**
   * Merges a response context into the current.
   * @throws IllegalStateException If a key of the {@code responseContext} has not been registered.
   */
  public void merge(ResponseContext responseContext)
  {
    responseContext.getDelegate().forEach((key, newValue) -> {
      if (newValue != null) {
        add(key, newValue);
      }
    });
  }

  /**
   * Serializes the context given that the resulting string length is less than the provided limit.
   * This method removes some elements from context collections if it's needed to satisfy the limit.
   * There is no explicit priorities of keys which values are being truncated because for now there are only
   * two potential limit breaking keys ({@link Key#UNCOVERED_INTERVALS}
   * and {@link Key#MISSING_SEGMENTS}) and their values are arrays.
   * Thus current implementation considers these arrays as equal prioritized and starts removing elements from
   * the array which serialized value length is the biggest.
   * The resulting string might be correctly deserialized to {@link ResponseContext}.
   */
  public SerializationResult serializeWith(ObjectMapper objectMapper, int maxCharsNumber) throws JsonProcessingException
  {
    final String fullSerializedString = objectMapper.writeValueAsString(getDelegate());
    if (fullSerializedString.length() <= maxCharsNumber) {
      return new SerializationResult(fullSerializedString, fullSerializedString);
    } else {
      // Indicates that the context is truncated during serialization.
      add(Key.TRUNCATED, true);
      final ObjectNode contextJsonNode = objectMapper.valueToTree(getDelegate());
      final ArrayList<Map.Entry<String, JsonNode>> sortedNodesByLength = Lists.newArrayList(contextJsonNode.fields());
      sortedNodesByLength.sort(VALUE_LENGTH_REVERSED_COMPARATOR);
      int needToRemoveCharsNumber = fullSerializedString.length() - maxCharsNumber;
      // The complexity of this block is O(n*m*log(m)) where n - context size, m - context's array size
      for (Map.Entry<String, JsonNode> e : sortedNodesByLength) {
        final String fieldName = e.getKey();
        final JsonNode node = e.getValue();
        if (node.isArray()) {
          if (needToRemoveCharsNumber >= node.toString().length()) {
            // We need to remove more chars than the field's length so removing it completely
            contextJsonNode.remove(fieldName);
            // Since the field is completely removed (name + value) we need to do a recalculation
            needToRemoveCharsNumber = contextJsonNode.toString().length() - maxCharsNumber;
          } else {
            final ArrayNode arrayNode = (ArrayNode) node;
            needToRemoveCharsNumber -= removeNodeElementsToSatisfyCharsLimit(arrayNode, needToRemoveCharsNumber);
            if (arrayNode.size() == 0) {
              // The field is empty, removing it because an empty array field may be misleading
              // for the recipients of the truncated response context.
              contextJsonNode.remove(fieldName);
              // Since the field is completely removed (name + value) we need to do a recalculation
              needToRemoveCharsNumber = contextJsonNode.toString().length() - maxCharsNumber;
            }
          } // node is not an array
        } else {
          // A context should not contain nulls so we completely remove the field.
          contextJsonNode.remove(fieldName);
          // Since the field is completely removed (name + value) we need to do a recalculation
          needToRemoveCharsNumber = contextJsonNode.toString().length() - maxCharsNumber;
        }
        if (needToRemoveCharsNumber <= 0) {
          break;
        }
      }
      return new SerializationResult(contextJsonNode.toString(), fullSerializedString);
    }
  }

  /**
   * Removes {@code node}'s elements which total length of serialized values is greater or equal to the passed limit.
   * If it is impossible to satisfy the limit the method removes all {@code node}'s elements.
   * On every iteration it removes exactly half of the remained elements to reduce the overall complexity.
   * @param node {@link ArrayNode} which elements are being removed.
   * @param needToRemoveCharsNumber the number of chars need to be removed.
   * @return the number of removed chars.
   */
  private static int removeNodeElementsToSatisfyCharsLimit(ArrayNode node, int needToRemoveCharsNumber)
  {
    int removedCharsNumber = 0;
    while (node.size() > 0 && needToRemoveCharsNumber > removedCharsNumber) {
      final int lengthBeforeRemove = node.toString().length();
      // Reducing complexity by removing half of array's elements
      final int removeUntil = node.size() / 2;
      for (int removeAt = node.size() - 1; removeAt >= removeUntil; removeAt--) {
        node.remove(removeAt);
      }
      final int lengthAfterRemove = node.toString().length();
      removedCharsNumber += lengthBeforeRemove - lengthAfterRemove;
    }
    return removedCharsNumber;
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
