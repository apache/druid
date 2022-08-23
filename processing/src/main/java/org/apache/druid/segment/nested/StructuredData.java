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
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.segment.column.TypeStrategies;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.LongSupplier;

public class StructuredData implements Comparable<StructuredData>
{
  private static final XXHash64 HASH_FUNCTION = XXHashFactory.fastestInstance().hash64();

  // seed from the example... but, it doesn't matter what it is as long as its the same every time
  private static int SEED = 0x9747b28c;

  public static final Comparator<StructuredData> COMPARATOR = Comparators.naturalNullsFirst();

  private static long computeHash(StructuredData data)
  {
    try {
      final byte[] bytes = NestedDataComplexTypeSerde.OBJECT_MAPPER.writeValueAsBytes(data.value);
      return HASH_FUNCTION.hash(bytes, 0, bytes.length, SEED);
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
  private final LongSupplier hash = () -> {
    if (!hashInitialized) {
      hashValue = computeHash(this);
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

  @Override
  public int compareTo(StructuredData o)
  {
    if (this.equals(o)) {
      return 0;
    }
    if (isNull()) {
      return -1;
    }
    if (o.isNull()) {
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
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(value);
  }

  @Override
  public String toString()
  {
    return "StructuredData{" +
           "value=" + value +
           '}';
  }
}
