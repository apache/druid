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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.indexer.Checks;
import org.apache.druid.indexer.Property;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * InputFormat for customized Delimitor Separate Value format of input data(default is TSV).
 */
public class DelimitedInputFormat extends FlatTextInputFormat
{
  private static final String DEFAULT_DELIMITER = "\t";
  private final String delimiter;

  @JsonCreator
  public DelimitedInputFormat(
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
        delimiter == null ? DEFAULT_DELIMITER : delimiter,
        hasHeaderRow,
        findColumnsFromHeader,
        skipHeaderRows
    );
    this.delimiter = delimiter;
  }

  @Override
  @JsonProperty
  public List<String> getColumns()
  {
    return super.getColumns();
  }

  @Override
  @JsonProperty
  public String getListDelimiter()
  {
    return super.getListDelimiter();
  }

  @JsonProperty("delimiter")
  public String getDelimiterString()
  {
    return delimiter;
  }

  @Override
  @JsonProperty
  public boolean isFindColumnsFromHeader()
  {
    return super.isFindColumnsFromHeader();
  }

  @Override
  @JsonProperty
  public int getSkipHeaderRows()
  {
    return super.getSkipHeaderRows();
  }

  @Override
  public boolean isSplittable()
  {
    return true;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return new DelimitedValueReader(
        inputRowSchema,
        source,
        temporaryDirectory,
        getListDelimiter(),
        getColumns(),
        isFindColumnsFromHeader(),
        getSkipHeaderRows(),
        getDelimiter()
    );
  }
}
