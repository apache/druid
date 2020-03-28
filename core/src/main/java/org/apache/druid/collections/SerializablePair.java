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

package org.apache.druid.collections;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.Pair;

import javax.annotation.Nullable;
import java.util.Comparator;

public class SerializablePair<T1, T2> extends Pair<T1, T2>
{
  @JsonCreator
  public SerializablePair(@JsonProperty("lhs") T1 lhs, @JsonProperty("rhs") @Nullable T2 rhs)
  {
    super(lhs, rhs);
  }

  @JsonProperty
  public T1 getLhs()
  {
    return lhs;
  }

  @JsonProperty
  @Nullable
  public T2 getRhs()
  {
    return rhs;
  }

  public static <T1, T2> Comparator<SerializablePair<T1, T2>> createNullHandlingComparator(
      Comparator<T2> delegate,
      boolean nullsFirst
  )
  {
    final int firstIsNull = nullsFirst ? -1 : 1;
    final int secondIsNull = nullsFirst ? 1 : -1;
    return (o1, o2) -> {
      if (o1 == null || o1.rhs == null) {
        if (o2 == null || o2.rhs == null) {
          return 0;
        }
        return firstIsNull;
      }
      if (o2 == null || o2.rhs == null) {
        return secondIsNull;
      }
      return delegate.compare(o1.rhs, o2.rhs);
    };
  }
}
