/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedObjectStrategy;

import java.util.Map;

public class IndexSpec
{
  private final Map<String, ColumnSpec> columnSpecs;
  private final BitmapSerdeFactory bitmapSerdeFactory;

  public IndexSpec()
  {
    this(null, null);
  }

  @JsonCreator
  public IndexSpec(
      @JsonProperty("columnSpecs") Map<String, ColumnSpec> columnSpecs,
      @JsonProperty("bitmapType") BitmapSerdeFactory bitmapSerdeFactory
  )
  {
    this.columnSpecs = columnSpecs == null
                       ? ImmutableMap.<String, ColumnSpec>of()
                       : columnSpecs;
    this.bitmapSerdeFactory = bitmapSerdeFactory == null
                              ? new BitmapSerde.DefaultBitmapSerdeFactory()
                              : bitmapSerdeFactory;
  }

  @JsonProperty("columnSpecs")
  public Map<String, ColumnSpec> getColumnSpecs()
  {
    return columnSpecs;
  }

  @JsonProperty("bitmapType")
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  public static ColumnSpec defaultColumnSpec() {
    return new ColumnSpec(null);
  }

  public static class ColumnSpec {
    private final CompressedObjectStrategy.CompressionStrategy compression;

    @JsonCreator
    public ColumnSpec(
        @JsonProperty("compression") CompressedObjectStrategy.CompressionStrategy compression
    )
    {
      this.compression = compression;
    }

    @JsonProperty("compression")
    public CompressedObjectStrategy.CompressionStrategy getCompression()
    {
      return compression;
    }
  }
}
