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

package org.apache.druid.delta.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import org.apache.druid.error.InvalidInput;

/**
 * Druid {@link DeltaFilter} that maps to a Delta predicate of type < for the supplied column and value.
 */
public class DeltaLessThanFilter implements DeltaFilter
{
  @JsonProperty
  private final String column;
  @JsonProperty
  private final String value;

  @JsonCreator
  public DeltaLessThanFilter(@JsonProperty("column") final String column, @JsonProperty("value") final String value)
  {
    if (column == null) {
      throw InvalidInput.exception("column is a required field for < filter.");
    }
    if (value == null) {
      throw InvalidInput.exception(
          "value is a required field for < filter. None provided for column[%s].", column
      );
    }
    this.column = column;
    this.value = value;
  }

  @Override
  public Predicate getFilterPredicate(StructType snapshotSchema)
  {
    return new Predicate(
        "<",
        ImmutableList.of(
            new Column(column),
            DeltaFilterUtils.dataTypeToLiteral(snapshotSchema, column, value)
        )
    );
  }
}
