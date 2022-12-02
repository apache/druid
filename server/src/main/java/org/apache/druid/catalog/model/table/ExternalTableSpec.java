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

package org.apache.druid.catalog.model.table;

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;

/**
 * Catalog form of an external table specification used to pass along the three
 * components needed for an external table in MSQ ingest. Just like
 * {@code ExternalTableSource}, except that the parameters are not required
 * to be non-null.
 */
public class ExternalTableSpec
{
  @Nullable public final InputSource inputSource;
  @Nullable public final InputFormat inputFormat;
  @Nullable public final RowSignature signature;

  public ExternalTableSpec(
      final InputSource inputSource,
      final InputFormat inputFormat,
      final RowSignature signature)
  {
    this.inputSource = inputSource;
    this.inputFormat = inputFormat;
    this.signature = signature;
  }
}
