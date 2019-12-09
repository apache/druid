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

package org.apache.druid.indexing.input;

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.IndexIO;

import java.io.File;
import java.util.List;

public class DruidSegmentInputFormat implements InputFormat
{
  private final IndexIO indexIO;
  private final DimFilter dimFilter;
  private List<String> dimensions;
  private List<String> metrics;

  DruidSegmentInputFormat(
      IndexIO indexIO,
      DimFilter dimFilter,
      List<String> dimensions,
      List<String> metrics
  )
  {
    this.indexIO = indexIO;
    this.dimFilter = dimFilter;
    this.dimensions = dimensions;
    this.metrics = metrics;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      File temporaryDirectory
  )
  {
    return new DruidSegmentReader(
        source,
        indexIO,
        dimensions,
        metrics,
        dimFilter,
        temporaryDirectory
    );
  }
}
