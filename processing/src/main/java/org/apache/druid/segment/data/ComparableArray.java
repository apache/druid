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

package org.apache.druid.segment.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.hash.Hashing;

import java.io.Serializable;
import java.util.Arrays;

public class ComparableArray implements Comparable<ComparableArray>, Serializable
{
  private static final ComparableArray EMPTY_ARRAY = new ComparableArray(new String[0]);
  final String[] delegate;

  private int hashCode;
  private boolean hashCodeComputed;

  private ComparableArray(String[] array)
  {
    delegate = array;
  }

  @JsonCreator
  public static ComparableArray of(String... array)
  {
    if (array.length == 0) {
      return EMPTY_ARRAY;
    } else {
      return new ComparableArray(array);
    }
  }

  // We only want to set the delegate arrray as part of the serialization
  @JsonValue
  public String[] getDelegate()
  {
    return delegate;
  }

  @Override
  public String toString()
  {
    return Arrays.toString(delegate);
  }

  @Override
  public int hashCode()
  {
    // Check is not thread-safe, but that's fine. Even if used by multiple threads, it's ok to write these primitive
    // fields more than once.
    if (!hashCodeComputed) {
      // Use murmur3_32 for dispersion properties needed by DistinctKeyCollector#isKeySelected.
      hashCode = Hashing.murmur3_32().hashInt(Arrays.hashCode(delegate)).asInt();
      hashCodeComputed = true;
    }

    return hashCode;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    return Arrays.equals(delegate, ((ComparableArray) obj).getDelegate());
  }

  @Override
  public int compareTo(ComparableArray rhs)
  {
    //noinspection ArrayEquality
    if (this.delegate == rhs.getDelegate()) {
      return 0;
    } else if (delegate.length > rhs.getDelegate().length) {
      return 1;
    } else if (delegate.length < rhs.getDelegate().length) {
      return -1;
    } else {
      for (int i = 0; i < delegate.length; i++) {
        final int cmp = delegate[i].compareTo(rhs.getDelegate()[i]);
        if (cmp == 0) {
          continue;
        }
        return cmp;
      }
      return 0;
    }
  }
}