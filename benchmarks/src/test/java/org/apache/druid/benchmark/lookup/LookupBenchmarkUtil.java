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

package org.apache.druid.benchmark.lookup;

import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.lookup.ImmutableLookupMap;
import org.apache.druid.query.lookup.LookupExtractor;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Utility functions for lookup benchmarks.
 */
public class LookupBenchmarkUtil
{
  /**
   * Length of keys. They are zero-padded to this size.
   */
  private static final int KEY_LENGTH = 20;

  public enum LookupType
  {
    HASHMAP {
      @Override
      public LookupExtractor build(Iterable<Pair<String, String>> keyValuePairs)
      {
        final Map<String, String> map = new HashMap<>();
        for (final Pair<String, String> keyValuePair : keyValuePairs) {
          map.put(keyValuePair.lhs, keyValuePair.rhs);
        }
        return new MapLookupExtractor(map, false);
      }
    },
    GUAVA {
      @Override
      public LookupExtractor build(Iterable<Pair<String, String>> keyValuePairs)
      {
        final ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
        for (final Pair<String, String> keyValuePair : keyValuePairs) {
          mapBuilder.put(keyValuePair.lhs, keyValuePair.rhs);
        }
        return new MapLookupExtractor(mapBuilder.build(), false);
      }
    },
    FASTUTIL {
      @Override
      public LookupExtractor build(Iterable<Pair<String, String>> keyValuePairs)
      {
        final Map<String, String> map = new Object2ObjectOpenHashMap<>();
        for (final Pair<String, String> keyValuePair : keyValuePairs) {
          map.put(keyValuePair.lhs, keyValuePair.rhs);
        }
        return new MapLookupExtractor(map, false);
      }
    },
    IMMUTABLE {
      @Override
      public LookupExtractor build(Iterable<Pair<String, String>> keyValuePairs)
      {
        final Map<String, String> map = new HashMap<>();
        for (final Pair<String, String> keyValuePair : keyValuePairs) {
          map.put(keyValuePair.lhs, keyValuePair.rhs);
        }
        return ImmutableLookupMap.fromMap(map).asLookupExtractor(false, () -> new byte[0]);
      }
    };

    public abstract LookupExtractor build(Iterable<Pair<String, String>> keyValuePairs);
  }

  private LookupBenchmarkUtil()
  {
    // No instantiation.
  }

  /**
   * Create a {@link LookupExtractor} for benchmarking. Keys are numbers from 0 (inclusive) to numKeys (exclusive),
   * as strings. Values are numbers from 0 (inclusive) to numValues (exclusive), cycled if numValues < numKeys.
   */
  public static LookupExtractor makeLookupExtractor(final LookupType lookupType, final int numKeys, final int numValues)
  {
    if (numValues > numKeys) {
      throw new IAE("numValues[%s] > numKeys[%s]", numValues, numKeys);
    }

    final Iterable<Pair<String, String>> keys =
        () -> IntStream.range(0, numKeys)
                       .mapToObj(i -> Pair.of(makeKeyOrValue(i), makeKeyOrValue(i % numValues)))
                       .iterator();

    return lookupType.build(keys);
  }

  public static String makeKeyOrValue(final int i)
  {
    return StringUtils.format("%0" + KEY_LENGTH + "d", i);
  }
}
