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


package org.apache.druid.segment.nested;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.data.BitmapSerdeFactory;

import java.nio.ByteOrder;

public class NestedDataColumnMetadata
{
  private final ByteOrder byteOrder;
  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final String fileNameBase;
  private final Boolean hasNulls;

  @JsonCreator
  public NestedDataColumnMetadata(
      @JsonProperty("byteOrder") ByteOrder byteOrder,
      @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory,
      @JsonProperty("fileNameBase") String fileNameBase,
      @JsonProperty("hasNulls") Boolean hasNulls
  )
  {
    this.byteOrder = byteOrder;
    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.fileNameBase = fileNameBase;
    this.hasNulls = hasNulls;
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  @JsonProperty
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  @JsonProperty
  public String getFileNameBase()
  {
    return fileNameBase;
  }

  @JsonProperty("hasNulls")
  public Boolean hasNulls()
  {
    return hasNulls;
  }
}
