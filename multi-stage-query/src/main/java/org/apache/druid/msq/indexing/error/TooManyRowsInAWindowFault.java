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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.msq.util.MultiStageQueryContext;

import java.util.Objects;

@JsonTypeName(TooManyRowsInAWindowFault.CODE)
public class TooManyRowsInAWindowFault extends BaseMSQFault
{

  static final String CODE = "TooManyRowsInAWindow";

  private final int numRows;
  private final int maxRows;

  @JsonCreator
  public TooManyRowsInAWindowFault(
      @JsonProperty("numRows") final int numRows,
      @JsonProperty("maxRows") final int maxRows
  )
  {
    super(
        CODE,
        "Too many rows in a window (requested = %d, max = %d). "
        + " Try creating a window with a higher cardinality column or change the query shape."
        + " Or you can change the max using query context param %s ."
        + " Use it carefully as a higher value can lead to OutOfMemory errors. ",
        numRows,
        maxRows,
        MultiStageQueryContext.MAX_ROWS_MATERIALIZED_IN_WINDOW
    );
    this.numRows = numRows;
    this.maxRows = maxRows;
  }

  @JsonProperty
  public int getNumRows()
  {
    return numRows;
  }

  @JsonProperty
  public int getMaxRows()
  {
    return maxRows;
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
    TooManyRowsInAWindowFault that = (TooManyRowsInAWindowFault) o;
    return numRows == that.numRows && maxRows == that.maxRows;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), numRows, maxRows);
  }
}
