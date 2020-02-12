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
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;

public class RegexInputFormat implements InputFormat
{
  private final String pattern;
  private final String listDelimiter;
  private final List<String> columns;

  @JsonCreator
  public RegexInputFormat(
      @JsonProperty("pattern") String pattern,
      @JsonProperty("listDelimiter") @Nullable String listDelimiter,
      @JsonProperty("columns") @Nullable List<String> columns
  )
  {
    this.pattern = pattern;
    this.listDelimiter = listDelimiter;
    this.columns = columns;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return new RegexReader(inputRowSchema, source, pattern, listDelimiter, columns);
  }
}
