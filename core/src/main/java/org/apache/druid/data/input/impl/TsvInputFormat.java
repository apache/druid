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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public class TsvInputFormat extends SeparateValueInputFormat
{

  private static Format getFormat(String delimiter)
  {
    if (delimiter != null && delimiter.length() > 0) {
      Format.CustomizeSV.setDelimiter(delimiter.charAt(0), null);
      return Format.CustomizeSV;
    } else {
      return Format.TSV;
    }
  }

  @JsonCreator
  public TsvInputFormat(
      @JsonProperty("columns") @Nullable List<String> columns,
      @JsonProperty("listDelimiter") @Nullable String listDelimiter,
      @JsonProperty("delimiter") @Nullable String delimiter,
      @Deprecated @JsonProperty("hasHeaderRow") @Nullable Boolean hasHeaderRow,
      @JsonProperty("findColumnsFromHeader") @Nullable Boolean findColumnsFromHeader,
      @JsonProperty("skipHeaderRows") int skipHeaderRows
  )
  {
    super(
        columns,
        listDelimiter,
        hasHeaderRow,
        findColumnsFromHeader,
        skipHeaderRows,
        getFormat(delimiter)
    );
  }
}
