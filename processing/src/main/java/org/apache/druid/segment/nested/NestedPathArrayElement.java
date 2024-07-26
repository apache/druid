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
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;


public class NestedPathArrayElement implements NestedPathPart
{
  private final int index;

  @JsonCreator
  public NestedPathArrayElement(@JsonProperty("index") int index)
  {
    this.index = index;
  }

  @Nullable
  @Override
  public Object find(@Nullable Object input)
  {
    // handle lists or arrays because who knows what might end up here, depending on how is created
    if (input instanceof List) {
      List<?> currentList = (List<?>) input;
      final int currentSize = currentList.size();
      if (index < 0) {
        final int adjusted = currentSize + index;
        if (adjusted >= 0) {
          return currentList.get(adjusted);
        }
      } else if (currentList.size() > index) {
        return currentList.get(index);
      }
    } else if (input instanceof Object[]) {
      Object[] currentList = (Object[]) input;
      if (index < 0) {
        final int adjusted = currentList.length + index;
        if (adjusted >= 0) {
          return currentList[adjusted];
        }
      } else if (currentList.length > index) {
        return currentList[index];
      }
    }
    return null;
  }

  @Override
  public String getPartIdentifier()
  {
    return String.valueOf(index);
  }

  @JsonProperty("index")
  public int getIndex()
  {
    return index;
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
    NestedPathArrayElement that = (NestedPathArrayElement) o;
    return index == that.index;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(index);
  }

  @Override
  public String toString()
  {
    return "NestedPathArrayElement{" +
           "index=" + index +
           '}';
  }
}
