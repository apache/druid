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
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.ConciseBitmapSerdeFactory;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * IndexSpec defines segment storage format options to be used at indexing time,
 * such as bitmap type, and column compression formats.
 *
 * IndexSpec is specified as part of the TuningConfig for the corresponding index task.
 */
public class IndexSpec
{
  public static final CompressedObjectStrategy.CompressionStrategy DEFAULT_METRIC_COMPRESSION = CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY;
  public static final CompressedObjectStrategy.CompressionStrategy DEFAULT_DIMENSION_COMPRESSION = CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY;
  public static final CompressionFactory.LongEncodingStrategy DEFAULT_LONG_ENCODING = CompressionFactory.DEFAULT_LONG_ENCODING_STRATEGY;

  private static final Set<CompressedObjectStrategy.CompressionStrategy> METRIC_COMPRESSION = Sets.newHashSet(
      Arrays.asList(CompressedObjectStrategy.CompressionStrategy.values())
  );

  private static final Set<CompressedObjectStrategy.CompressionStrategy> DIMENSION_COMPRESSION = Sets.newHashSet(
      Arrays.asList(CompressedObjectStrategy.CompressionStrategy.noNoneValues())
  );

  private static final Set<CompressionFactory.LongEncodingStrategy> LONG_ENCODING_NAMES = Sets.newHashSet(
      Arrays.asList(CompressionFactory.LongEncodingStrategy.values())
  );

  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final CompressedObjectStrategy.CompressionStrategy dimensionCompression;
  private final CompressedObjectStrategy.CompressionStrategy metricCompression;
  private final CompressionFactory.LongEncodingStrategy longEncoding;


  /**
   * Creates an IndexSpec with default parameters
   */
  public IndexSpec()
  {
    this(null, null, null, null);
  }

  /**
   * Creates an IndexSpec with the given storage format settings.
   *
   *
   * @param bitmapSerdeFactory type of bitmap to use (e.g. roaring or concise), null to use the default.
   *                           Defaults to the bitmap type specified by the (deprecated) "druid.processing.bitmap.type"
   *                           setting, or, if none was set, uses the default defined in {@link BitmapSerde}
   *
   * @param dimensionCompression compression format for dimension columns, null to use the default.
   *                             Defaults to {@link CompressedObjectStrategy#DEFAULT_COMPRESSION_STRATEGY}
   *
   * @param metricCompression compression format for metric columns, null to use the default.
   *                          Defaults to {@link CompressedObjectStrategy#DEFAULT_COMPRESSION_STRATEGY}
   *
   * @param longEncoding encoding strategy for metric and dimension columns with type long, null to use the default.
   *                     Defaults to {@link CompressionFactory#DEFAULT_LONG_ENCODING_STRATEGY}
   */
  @JsonCreator
  public IndexSpec(
      @JsonProperty("bitmap") BitmapSerdeFactory bitmapSerdeFactory,
      @JsonProperty("dimensionCompression") CompressedObjectStrategy.CompressionStrategy dimensionCompression,
      @JsonProperty("metricCompression") CompressedObjectStrategy.CompressionStrategy metricCompression,
      @JsonProperty("longEncoding") CompressionFactory.LongEncodingStrategy longEncoding
  )
  {
    Preconditions.checkArgument(dimensionCompression == null || DIMENSION_COMPRESSION.contains(dimensionCompression),
                                "Unknown compression type[%s]", dimensionCompression);

    Preconditions.checkArgument(metricCompression == null || METRIC_COMPRESSION.contains(metricCompression),
                                "Unknown compression type[%s]", metricCompression);

    Preconditions.checkArgument(longEncoding == null || LONG_ENCODING_NAMES.contains(longEncoding),
                                "Unknown long encoding type[%s]", longEncoding);

    this.bitmapSerdeFactory = bitmapSerdeFactory != null ? bitmapSerdeFactory : new ConciseBitmapSerdeFactory();
    this.dimensionCompression = dimensionCompression == null ?DEFAULT_DIMENSION_COMPRESSION : dimensionCompression;
    this.metricCompression = metricCompression == null ? DEFAULT_METRIC_COMPRESSION : metricCompression;
    this.longEncoding = longEncoding == null ? DEFAULT_LONG_ENCODING : longEncoding;
  }

  @JsonProperty("bitmap")
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  @JsonProperty
  public CompressedObjectStrategy.CompressionStrategy getDimensionCompression()
  {
    return dimensionCompression;
  }

  @JsonProperty
  public CompressedObjectStrategy.CompressionStrategy getMetricCompression()
  {
    return metricCompression;
  }

  @JsonProperty
  public CompressionFactory.LongEncodingStrategy getLongEncoding()
  {
    return longEncoding;
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
    return Objects.equals(bitmapSerdeFactory, indexSpec.bitmapSerdeFactory) &&
           dimensionCompression == indexSpec.dimensionCompression &&
           metricCompression == indexSpec.metricCompression &&
           longEncoding == indexSpec.longEncoding;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bitmapSerdeFactory, dimensionCompression, metricCompression, longEncoding);
  }

  @Override
  public String toString()
  {
    return "IndexSpec{" +
           "bitmapSerdeFactory=" + bitmapSerdeFactory +
           ", dimensionCompression=" + dimensionCompression +
           ", metricCompression=" + metricCompression +
           ", longEncoding=" + longEncoding +
           '}';
  }
}
