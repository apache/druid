/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Set;

/**
 * IndexSpec defines segment storage format options to be used at indexing time,
 * such as bitmap type, and column compression formats.
 *
 * IndexSpec is specified as part of the TuningConfig for the corresponding index task.
 */
public class IndexSpec
{
  public static final String UNCOMPRESSED = "uncompressed";
  public static final String DEFAULT_METRIC_COMPRESSION = CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY.name().toLowerCase();
  public static final String DEFAULT_DIMENSION_COMPRESSION = CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY.name().toLowerCase();

  private static final Set<String> COMPRESSION_NAMES = Sets.newHashSet(
      Iterables.transform(
          Arrays.asList(CompressedObjectStrategy.CompressionStrategy.values()),
          new Function<CompressedObjectStrategy.CompressionStrategy, String>()
          {
            @Nullable
            @Override
            public String apply(CompressedObjectStrategy.CompressionStrategy input)
            {
              return input.name().toLowerCase();
            }
          }
      )
  );

  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final String dimensionCompression;
  private final String metricCompression;


  /**
   * Creates an IndexSpec with default parameters
   */
  public IndexSpec()
  {
    this(null, null, null);
  }

  /**
   * Creates an IndexSpec with the given storage format settings.
   *
   *
   * @param bitmapSerdeFactory type of bitmap to use (e.g. roaring or concise), null to use the default.
   *                           Defaults to the bitmap type specified by the (deprecated) "druid.processing.bitmap.type"
   *                           setting, or, if none was set, uses the default @{link BitmapSerde.DefaultBitmapSerdeFactory}
   *
   * @param dimensionCompression compression format for dimension columns, null to use the default
   *                             Defaults to @{link CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY}
   *
   * @param metricCompression compression format for metric columns, null to use the default.
   *                          Defaults to @{link CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY}
   */
  @JsonCreator
  public IndexSpec(
      @JsonProperty("bitmap") BitmapSerdeFactory bitmapSerdeFactory,
      @JsonProperty("dimensionCompression") String dimensionCompression,
      @JsonProperty("metricCompression") String metricCompression
  )
  {
    Preconditions.checkArgument(dimensionCompression == null || dimensionCompression.equals(UNCOMPRESSED) || COMPRESSION_NAMES.contains(dimensionCompression),
                                "Unknown compression type[%s]", dimensionCompression);

    Preconditions.checkArgument(metricCompression == null || COMPRESSION_NAMES.contains(metricCompression),
                                "Unknown compression type[%s]", metricCompression);

    this.bitmapSerdeFactory = bitmapSerdeFactory != null ? bitmapSerdeFactory : new ConciseBitmapSerdeFactory();
    this.metricCompression = metricCompression;
    this.dimensionCompression = dimensionCompression;
  }

  @JsonProperty("bitmap")
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  @JsonProperty("dimensionCompression")
  public String getDimensionCompression()
  {
    return dimensionCompression;
  }

  @JsonProperty("metricCompression")
  public String getMetricCompression()
  {
    return metricCompression;
  }

  public CompressedObjectStrategy.CompressionStrategy getMetricCompressionStrategy()
  {
    return CompressedObjectStrategy.CompressionStrategy.valueOf(
        (metricCompression == null ? DEFAULT_METRIC_COMPRESSION : metricCompression).toUpperCase()
    );
  }

  public CompressedObjectStrategy.CompressionStrategy getDimensionCompressionStrategy()
  {
    return dimensionCompression == null ?
           dimensionCompressionStrategyForName(DEFAULT_DIMENSION_COMPRESSION) :
           dimensionCompressionStrategyForName(dimensionCompression);
  }

  private static CompressedObjectStrategy.CompressionStrategy dimensionCompressionStrategyForName(String compression)
  {
    return compression.equals(UNCOMPRESSED) ? null :
           CompressedObjectStrategy.CompressionStrategy.valueOf(compression.toUpperCase());
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
    if (dimensionCompression != null
        ? !dimensionCompression.equals(indexSpec.dimensionCompression)
        : indexSpec.dimensionCompression != null) {
      return false;
    }
    return !(metricCompression != null
             ? !metricCompression.equals(indexSpec.metricCompression)
             : indexSpec.metricCompression != null);

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
