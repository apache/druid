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

package org.apache.druid.catalog.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * JSON payload for the reorder column API.
 */
public class MoveColumn
{
  public enum Position
  {
    FIRST,
    LAST,
    BEFORE,
    AFTER
  }

  @JsonProperty
  public final String column;
  @JsonProperty
  public final Position where;
  @Nullable
  @JsonProperty
  public final String anchor;

  @JsonCreator
  public MoveColumn(
      @JsonProperty("column") final String column,
      @JsonProperty("where") final Position where,
      @JsonProperty("anchor") @Nullable final String anchor
  )
  {
    this.column = column;
    this.where = where;
    this.anchor = anchor;
  }

  public List<ColumnSpec> perform(List<ColumnSpec> columns)
  {
    List<ColumnSpec> revised = new ArrayList<>(columns);
    final int colPosn = findColumn(columns, column);
    if (colPosn == -1) {
      throw new ISE("Column [%s] is not defined", column);
    }
    int anchorPosn;
    if (where == Position.BEFORE || where == Position.AFTER) {
      anchorPosn = findColumn(columns, anchor);
      if (anchorPosn == -1) {
        throw new ISE("Anchor [%s] is not defined", column);
      }
      if (anchorPosn > colPosn) {
        anchorPosn--;
      }
    } else {
      anchorPosn = -1;
    }

    ColumnSpec col = revised.remove(colPosn);
    switch (where) {
      case FIRST:
        revised.add(0, col);
        break;
      case LAST:
        revised.add(col);
        break;
      case BEFORE:
        revised.add(anchorPosn, col);
        break;
      case AFTER:
        revised.add(anchorPosn + 1, col);
        break;
    }
    return revised;
  }

  private static int findColumn(List<ColumnSpec> columns, String colName)
  {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).name().equals(colName)) {
        return i;
      }
    }
    return -1;
  }


  @Override
  public boolean equals(Object o)
  {
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    MoveColumn other = (MoveColumn) o;
    return Objects.equals(this.column, other.column)
        && this.where == other.where
        && Objects.equals(this.anchor, other.anchor);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(column, where, anchor);
  }
}
