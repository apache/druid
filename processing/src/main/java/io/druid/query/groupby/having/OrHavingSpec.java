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

import java.util.List;
import java.util.Map;

/**
 * The logical "or" operator for the "having" clause.
 */
public class OrHavingSpec extends BaseHavingSpec
{
  private final List<HavingSpec> havingSpecs;

  @JsonCreator
  public OrHavingSpec(@JsonProperty("havingSpecs") List<HavingSpec> havingSpecs)
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
      if (havingSpec.eval(row)) {
        return true;
      }
    }

    return false;
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

    OrHavingSpec that = (OrHavingSpec) o;

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
    sb.append("OrHavingSpec");
    sb.append("{havingSpecs=").append(havingSpecs);
    sb.append('}');
    return sb.toString();
  }
}
