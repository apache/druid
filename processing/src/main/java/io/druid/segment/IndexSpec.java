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
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;

import java.util.Map;

public class IndexSpec
{
  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final CompressedObjectStrategy.CompressionStrategy dimensionCompression;
  private final CompressedObjectStrategy.CompressionStrategy metricCompression;

  public IndexSpec()
  {
    this(null, null, null);
  }

  @JsonCreator
  public IndexSpec(
      @JsonProperty("bitmapType") BitmapSerdeFactory bitmapSerdeFactory,
      @JsonProperty("dimensionCompression") CompressedObjectStrategy.CompressionStrategy dimensionCompression,
      @JsonProperty("metricCompression") CompressedObjectStrategy.CompressionStrategy metricCompression
  )
  {
    if (bitmapSerdeFactory != null) {
      this.bitmapSerdeFactory = bitmapSerdeFactory;
    } else {
      this.bitmapSerdeFactory = IndexIO.CONFIGURED_BITMAP_SERDE_FACTORY;
    }
    this.metricCompression = metricCompression != null ? metricCompression : CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY;
    this.dimensionCompression = dimensionCompression;
  }

  @JsonProperty("bitmapType")
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  @JsonProperty("dimensionCompression")
  public CompressedObjectStrategy.CompressionStrategy getDimensionCompression()
  {
    return dimensionCompression;
  }

  @JsonProperty("metricCompression")
  public CompressedObjectStrategy.CompressionStrategy getMetricCompression()
  {
    return metricCompression;
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

    IndexSpec indexSpec = (IndexSpec) o;

    if (bitmapSerdeFactory != null
        ? !bitmapSerdeFactory.equals(indexSpec.bitmapSerdeFactory)
        : indexSpec.bitmapSerdeFactory != null) {
      return false;
    }
    if (dimensionCompression != indexSpec.dimensionCompression) {
      return false;
    }
    return metricCompression == indexSpec.metricCompression;

  }

  @Override
  public int hashCode()
  {
    int result = bitmapSerdeFactory != null ? bitmapSerdeFactory.hashCode() : 0;
    result = 31 * result + (dimensionCompression != null ? dimensionCompression.hashCode() : 0);
    result = 31 * result + (metricCompression != null ? metricCompression.hashCode() : 0);
    return result;
  }
}
