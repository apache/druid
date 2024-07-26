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

package org.apache.druid.query.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

public class NaiveSortOperatorFactory implements OperatorFactory
{
  private final List<ColumnWithDirection> sortColumns;

  @JsonCreator
  public NaiveSortOperatorFactory(
      @JsonProperty("columns") List<ColumnWithDirection> sortColumns
  )
  {
    this.sortColumns = sortColumns;
  }

  @JsonProperty("columns")
  public List<ColumnWithDirection> getSortColumns()
  {
    return sortColumns;
  }

  @Override
  public Operator wrap(Operator op)
  {
    return new NaiveSortOperator(op, sortColumns);
  }

  @Override
  public boolean validateEquivalent(OperatorFactory other)
  {
    if (other instanceof NaiveSortOperatorFactory) {
      return sortColumns.equals(((NaiveSortOperatorFactory) other).getSortColumns());
    }
    return false;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(sortColumns);
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
    NaiveSortOperatorFactory other = (NaiveSortOperatorFactory) obj;
    return Objects.equals(sortColumns, other.sortColumns);
  }

  @Override
  public String toString()
  {
    return "NaiveSortOperatorFactory{sortColumns=" + sortColumns + "}";
  }
}
