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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ForwardingList;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.IAE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * List of range partition boundaries.
 */
public class PartitionBoundaries extends ForwardingList<StringTuple> implements List<StringTuple>
{
  private final List<StringTuple> delegate;

  /**
   * @param partitions Elements corresponding to evenly-spaced fractional ranks of the distribution
   */
  public PartitionBoundaries(StringTuple... partitions)
  {
    if (partitions.length == 0) {
      delegate = Collections.emptyList();
      return;
    }

    // Future improvement: Handle skewed partitions better (e.g., many values are repeated).
    List<StringTuple> partitionBoundaries = Arrays.stream(partitions)
                                             .distinct()
                                             .collect(Collectors.toCollection(ArrayList::new));

    // First partition starts with null (see StringPartitionChunk.isStart())
    partitionBoundaries.set(0, null);

    // Last partition ends with null (see StringPartitionChunk.isEnd())
    if (partitionBoundaries.size() == 1) {
      partitionBoundaries.add(null);
    } else {
      partitionBoundaries.set(partitionBoundaries.size() - 1, null);
    }

    delegate = Collections.unmodifiableList(partitionBoundaries);
  }

  /**
   * This constructor supports an array of Objects and not just an array of
   * StringTuples for backward compatibility. Older versions of this class
   * are serialized as a String array.
   *
   * @param partitions array of StringTuples or array of String
   */
  @JsonCreator
  private PartitionBoundaries(Object[] partitions)
  {
    delegate = Arrays.stream(partitions)
                     .map(this::toStringTuple)
                     .collect(Collectors.toList());
  }

  @JsonValue
  public Object getSerializableObject()
  {
    boolean isSingleDim = true;
    for (StringTuple tuple : delegate) {
      if (tuple != null && tuple.size() != 1) {
        isSingleDim = false;
        break;
      }
    }

    if (isSingleDim) {
      return delegate.stream().map(StringTuple::firstOrNull).collect(Collectors.toList());
    } else {
      return delegate;
    }
  }

  /**
   * Converts the given item to a StringTuple.
   */
  private StringTuple toStringTuple(Object item)
  {
    if (item == null || item instanceof StringTuple) {
      return (StringTuple) item;
    } else if (item instanceof String) {
      return StringTuple.create((String) item);
    } else if (item instanceof String[]) {
      return StringTuple.create((String[]) item);
    } else if (item instanceof List) {
      return StringTuple.create((String[]) ((List) item).toArray(new String[0]));
    } else {
      throw new IAE("Item must either be a String or StringTuple");
    }
  }

  @Override
  protected List<StringTuple> delegate()
  {
    return delegate;
  }

  public int getNumBuckets()
  {
    return delegate.size() - 1;
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
    if (!super.equals(o)) {
      return false;
    }
    PartitionBoundaries strings = (PartitionBoundaries) o;
    return Objects.equals(delegate, strings.delegate);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), delegate);
  }
}
