/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.druid.data.input.Row;
import io.druid.segment.column.ValueType;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * The logical "and" operator for the "having" clause.
 */
public class AndHavingSpec extends BaseHavingSpec
{
  private static final byte CACHE_KEY = 0x2;

  private List<HavingSpec> havingSpecs;

  @JsonCreator
  public AndHavingSpec(@JsonProperty("havingSpecs") List<HavingSpec> havingSpecs)
  {
    this.havingSpecs = havingSpecs == null ? ImmutableList.<HavingSpec>of() : havingSpecs;
  }

  @JsonProperty("havingSpecs")
  public List<HavingSpec> getHavingSpecs()
  {
    return havingSpecs;
  }

  @Override
  public void setRowSignature(Map<String, ValueType> rowSignature)
  {
    for (HavingSpec havingSpec : havingSpecs) {
      havingSpec.setRowSignature(rowSignature);
    }
  }

  @Override
  public boolean eval(Row row)
  {
    for (HavingSpec havingSpec : havingSpecs) {
      if (!havingSpec.eval(row)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[][] havingBytes = new byte[havingSpecs.size()][];
    int havingBytesSize = 0;
    int index = 0;
    for (HavingSpec havingSpec : havingSpecs) {
      havingBytes[index] = havingSpec.getCacheKey();
      havingBytesSize += havingBytes[index].length;
      ++index;
    }

    ByteBuffer buffer = ByteBuffer.allocate(1 + havingBytesSize).put(CACHE_KEY);
    for (byte[] havingByte : havingBytes) {
      buffer.put(havingByte);
    }
    return buffer.array();
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

    AndHavingSpec that = (AndHavingSpec) o;

    if (havingSpecs != null ? !havingSpecs.equals(that.havingSpecs) : that.havingSpecs != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return havingSpecs != null ? havingSpecs.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("AndHavingSpec");
    sb.append("{havingSpecs=").append(havingSpecs);
    sb.append('}');
    return sb.toString();
  }
}
