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

package org.apache.druid.segment.nested;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.serde.ColumnSerializerUtils;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.function.LongSupplier;

public class StructuredData implements Comparable<StructuredData>
{
  private static final XXHash64 HASH_FUNCTION = XXHashFactory.fastestInstance().hash64();

  // seed from the example... but, it doesn't matter what it is as long as it's the same every time
  private static int SEED = 0x9747b28c;

  public static final Comparator<StructuredData> COMPARATOR = Comparators.naturalNullsFirst();

  /**
   * SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS is required so that hash computations for JSON objects that
   * have different key orders but are otherwise equivalent will be consistent.
   */
  private static final ObjectWriter WRITER = ColumnSerializerUtils.SMILE_MAPPER.writer(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

  private static byte[] serialized(StructuredData data)
  {
    try {
      return WRITER.writeValueAsBytes(data.value);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Nullable
  public static StructuredData wrap(@Nullable Object value)
  {
    if (value == null || value instanceof StructuredData) {
      return (StructuredData) value;
    }
    return new StructuredData(value);
  }

  @Nullable
  public static Object unwrap(@Nullable Object value)
  {
    if (value instanceof StructuredData) {
      return ((StructuredData) value).getValue();
    }
    return value;
  }

  @JsonCreator
  public static StructuredData create(Object value)
  {
    return new StructuredData(value);
  }

  private final Object value;
  private volatile boolean hashInitialized = false;
  private volatile long hashValue;
  private volatile int sizeEstimate = -1;
  private final LongSupplier hash = () -> {
    if (!hashInitialized) {
      final byte[] bytes = serialized(this);
      // compute the size estimate, note it's not an accurate representation of the heap size
      sizeEstimate = bytes.length + Integer.BYTES; // add 4 bytes for the length prefix
      // compute the hash, we might use it for comparison later
      hashValue = HASH_FUNCTION.hash(bytes, 0, bytes.length, SEED);
      hashInitialized = true;
    }
    return hashValue;
  };

  public StructuredData(Object value)
  {
    this.value = value;
  }

  public Object getValue()
  {
    return value;
  }

  private boolean isNull()
  {
    return value == null;
  }

  private boolean isString()
  {
    return value instanceof String;
  }

  private boolean isNumber()
  {
    return value instanceof Number;
  }

  private String asString()
  {
    return (String) value;
  }

  private Number asNumber()
  {
    return (Number) value;
  }

  public int getSizeEstimate()
  {
    if (sizeEstimate < 0) {
      hash.getAsLong(); // trigger hash computation which also sets sizeEstimate
    }
    Preconditions.checkState(sizeEstimate >= 0, "sizeEstimate not initialized");
    return sizeEstimate;
  }

  @Override
  public int compareTo(StructuredData o)
  {
    if (this == o) {
      return 0;
    } else if (o == null) {
      return 1;
    }

    if (isNull() && o.isNull()) {
      return 0;
    } else if (isNull()) {
      return -1;
    } else if (o.isNull()) {
      return 1;
    }

    // string before numbers and objects
    if (isString()) {
      if (o.isString()) {
        return TypeStrategies.STRING.compare(asString(), o.asString());
      }
      return -1;
    }
    if (o.isString()) {
      return 1;
    }

    // numbers before objects
    if (isNumber()) {
      if (o.isNumber()) {
        return TypeStrategies.DOUBLE.compare(asNumber().doubleValue(), o.asNumber().doubleValue());
      }
      return -1;
    }
    if (o.isNumber()) {
      return 1;
    }

    // finally compare hashes. there is a small chance of collisions for objects that are not equal but have the
    // same hash, we could revisit this later if needed
    return Long.compare(hash.getAsLong(), o.hash.getAsLong());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StructuredData that = (StructuredData) o;
    // guarantees that equals is consistent with compareTo
    return compareTo(that) == 0;
  }

  @Override
  public int hashCode()
  {
    return Longs.hashCode(hash.getAsLong());
  }

  @Override
  public String toString()
  {
    return "StructuredData{" +
           "value=" + value +
           '}';
  }
}
