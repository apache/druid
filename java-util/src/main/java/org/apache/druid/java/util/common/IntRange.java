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
package org.apache.druid.java.util.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class IntRange
{
  private final int start;
  private final int len;

  @JsonCreator
  public IntRange(
      @JsonProperty("start") int start,
      @JsonProperty("len") int len
  )
  {
    this.start = start;
    this.len = len;
  }

  public boolean contains(int i)
  {
    return i >= start && i < getExclusiveEnd();
  }

  @JsonProperty
  public int getStart()
  {
    return start;
  }

  @JsonProperty
  public int getLen()
  {
    return len;
  }

  public int getExclusiveEnd()
  {
    return start + len;
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
    IntRange intRange = (IntRange) o;
    return start == intRange.start &&
           len == intRange.len;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(start, len);
  }

  @Override
  public String toString()
  {
    return "IntRange{" +
           "start=" + start +
           ", len=" + len +
           '}';
  }
}
