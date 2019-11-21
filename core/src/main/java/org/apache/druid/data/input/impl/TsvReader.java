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

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRowSchema;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;

public class TsvReader extends SeperateValueReader
{
  TsvReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      File temporaryDirectory,
      @Nullable String listDelimiter,
      @Nullable List<String> columns,
      boolean findColumnsFromHeader,
      int skipHeaderRows
  )
  {
    super(
        inputRowSchema,
        source,
        temporaryDirectory,
        listDelimiter,
        columns,
        findColumnsFromHeader,
        skipHeaderRows,
        "tab"
    );
  }
}
