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

import java.util.ArrayList;

public class NaiveSortOperatorFactory implements OperatorFactory
{
  private final ArrayList<ColumnWithDirection> sortColumns;

  @JsonCreator
  public NaiveSortOperatorFactory(
      @JsonProperty("columns") ArrayList<ColumnWithDirection> sortColumns
  )
  {
    this.sortColumns = sortColumns;
  }

  @JsonProperty("columns")
  public ArrayList<ColumnWithDirection> getSortColumns()
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
}
