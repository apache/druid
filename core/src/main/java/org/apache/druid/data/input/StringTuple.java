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

package org.apache.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;

import java.util.Arrays;

/**
 * Represents a tuple of String values, typically used to represent
 * (single-valued) dimension values for an InputRow.
 */
public class StringTuple implements Comparable<StringTuple>
{
  private final String[] values;

  public static StringTuple create(String... values)
  {
    return new StringTuple(values);
  }

  /**
   * Gets the first String from the given StringTuple if the tuple is non-null
   * and non-empty, null otherwise.
   */
  public static String firstOrNull(StringTuple tuple)
  {
    return tuple == null || tuple.size() < 1 ? null : tuple.get(0);
  }

  @JsonCreator
  public StringTuple(String[] values)
  {
    Preconditions.checkNotNull(values, "Array of values should not be null");
    this.values = values;
  }

  public String get(int index)
  {
    return values[index];
  }

  public int size()
  {
    return values.length;
  }

  @JsonValue
  public String[] toArray()
  {
    return values;
  }

  @Override
  public int compareTo(StringTuple that)
  {
    // null is less than non-null
    if (this == that) {
      return 0;
    } else if (that == null) {
      return 1;
    }

    // Compare tuples of the same size only
    if (size() != that.size()) {
      throw new IAE("Cannot compare StringTuples of different sizes");
    }

    // Both tuples are empty
    if (size() == 0) {
      return 0;
    }

    // Compare the elements at each index until a differing element is found
    for (int i = 0; i < size(); ++i) {
      int comparison = nullSafeCompare(get(i), that.get(i));
      if (comparison != 0) {
        return comparison;
      }
    }

    return 0;
  }

  private int nullSafeCompare(String a, String b)
  {
    // Treat null as smaller than non-null
    if (a == null && b == null) {
      return 0;
    } else if (a == null) {
      return -1;
    } else if (b == null) {
      return 1;
    } else {
      return a.compareTo(b);
    }
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
    StringTuple that = (StringTuple) o;
    return Arrays.equals(values, that.values);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(values);
  }

  @Override
  public String toString()
  {
    return Arrays.toString(values);
  }

}
