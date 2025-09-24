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

package org.apache.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.BitmapSerde;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.nested.NestedCommonFormatColumnFormatSpec;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * IndexSpec defines segment storage format options to be used at indexing time,
 * such as bitmap type, and column compression formats.
 * <p>
 * IndexSpec is specified as part of the TuningConfig for the corresponding index task. Fields which are not explicitly
 * defined by the operator default to null values, callers must call {@link #getEffectiveSpec()} to fill in these null
 * values, which will replace nulls first with any system defaults defined in {@link #getDefault()} falling back to
 * hard coded defaults.
 */
public class IndexSpec
{
  public static IndexSpec getDefault()
  {
    return BuiltInTypesModule.getDefaultIndexSpec();
  }

  public static Builder builder()
  {
    return new Builder();
  }

  @Nullable
  private final BitmapSerdeFactory bitmapSerdeFactory;
  @Nullable
  private final CompressionStrategy dimensionCompression;
  @Nullable
  private final StringEncodingStrategy stringDictionaryEncoding;
  @Nullable
  private final CompressionStrategy metricCompression;
  @Nullable
  private final CompressionFactory.LongEncodingStrategy longEncoding;
  @Nullable
  private final CompressionStrategy complexMetricCompression;
  @Nullable
  private final CompressionStrategy jsonCompression;
  @Nullable
  private final SegmentizerFactory segmentLoader;
  @Nullable
  private final NestedCommonFormatColumnFormatSpec autoColumnFormatSpec;

  /**
   * Creates an IndexSpec with the given storage format settings.
   *
   * @param bitmapSerdeFactory       type of bitmap to use (e.g. roaring or concise), null to use the default.
   *                                 Defaults to the bitmap type specified by the (deprecated)
   *                                 "druid.processing.bitmap.type" setting, or, if none was set, uses the default
   *                                 defined in {@link BitmapSerde} upon calling {@link #getEffectiveSpec()}
   * @param dimensionCompression     compression format for dimension columns, null to use the default.
   *                                 Defaults to {@link CompressionStrategy#DEFAULT_COMPRESSION_STRATEGY} upon calling
   *                                 {@link #getEffectiveSpec()}
   * @param stringDictionaryEncoding encoding strategy for string dictionaries of dictionary encoded string columns
   * @param metricCompression        compression format for primitive type metric columns, null to use the default.
   *                                 Defaults to {@link CompressionStrategy#DEFAULT_COMPRESSION_STRATEGY} upon calling
   *                                 {@link #getEffectiveSpec()}.
   * @param longEncoding             encoding strategy for metric and dimension columns with type long, null to use the
   *                                 default. Defaults to {@link CompressionFactory#DEFAULT_LONG_ENCODING_STRATEGY} upon
   *                                 calling {@link #getEffectiveSpec()}
   * @param complexMetricCompression default {@link CompressionStrategy} to use for complex type columns which use
   *                                 generic serializers. Defaults to null which means no compression upon calling
   *                                 {@link #getEffectiveSpec()}.
   * @param segmentLoader            specify a {@link SegmentizerFactory} which will be written to 'factory.json' and
   *                                 used to load the written segment
   * @param autoColumnFormatSpec     specify the default {@link NestedCommonFormatColumnFormatSpec} to use for json and
   *                                 auto columns. Defaults to null upon calling {@link #getEffectiveSpec()}.
   */
  @JsonCreator
  public IndexSpec(
      @JsonProperty("bitmap") @Nullable BitmapSerdeFactory bitmapSerdeFactory,
      @JsonProperty("dimensionCompression") @Nullable CompressionStrategy dimensionCompression,
      @JsonProperty("stringDictionaryEncoding") @Nullable StringEncodingStrategy stringDictionaryEncoding,
      @JsonProperty("metricCompression") @Nullable CompressionStrategy metricCompression,
      @JsonProperty("longEncoding") @Nullable CompressionFactory.LongEncodingStrategy longEncoding,
      @JsonProperty("complexMetricCompression") @Nullable CompressionStrategy complexMetricCompression,
      @Deprecated @JsonProperty("jsonCompression") @Nullable CompressionStrategy jsonCompression,
      @JsonProperty("segmentLoader") @Nullable SegmentizerFactory segmentLoader,
      @JsonProperty("autoColumnFormatSpec") @Nullable NestedCommonFormatColumnFormatSpec autoColumnFormatSpec
  )
  {
    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.dimensionCompression = dimensionCompression;
    this.stringDictionaryEncoding = stringDictionaryEncoding;
    this.metricCompression = metricCompression;
    this.complexMetricCompression = complexMetricCompression;
    this.longEncoding = longEncoding;
    this.jsonCompression = jsonCompression;
    this.segmentLoader = segmentLoader;
    this.autoColumnFormatSpec = autoColumnFormatSpec;
  }

  @JsonProperty("bitmap")
  @Nullable
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  @JsonProperty
  @Nullable
  public CompressionStrategy getDimensionCompression()
  {
    return dimensionCompression;
  }

  @JsonProperty
  @Nullable
  public StringEncodingStrategy getStringDictionaryEncoding()
  {
    return stringDictionaryEncoding;
  }

  @JsonProperty
  @Nullable
  public CompressionStrategy getMetricCompression()
  {
    return metricCompression;
  }

  @JsonProperty
  @Nullable
  public CompressionFactory.LongEncodingStrategy getLongEncoding()
  {
    return longEncoding;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public CompressionStrategy getComplexMetricCompression()
  {
    return complexMetricCompression;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public SegmentizerFactory getSegmentLoader()
  {
    return segmentLoader;
  }

  @Deprecated
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public CompressionStrategy getJsonCompression()
  {
    return jsonCompression;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public NestedCommonFormatColumnFormatSpec getAutoColumnFormatSpec()
  {
    return autoColumnFormatSpec;
  }

  /**
   * Populate all null fields of {@link IndexSpec}, first from {@link #getDefault()} and finally falling back to hard
   * coded defaults if no overrides are defined.
   */
  @JsonIgnore
  public IndexSpec getEffectiveSpec()
  {
    Builder bob = IndexSpec.builder();
    final IndexSpec defaultSpec = getDefault();

    if (bitmapSerdeFactory != null) {
      bob.withBitmapSerdeFactory(bitmapSerdeFactory);
    } else if (defaultSpec.bitmapSerdeFactory != null) {
      bob.withBitmapSerdeFactory(defaultSpec.bitmapSerdeFactory);
    } else {
      bob.withBitmapSerdeFactory(new BitmapSerde.DefaultBitmapSerdeFactory());
    }

    if (dimensionCompression != null) {
      bob.withDimensionCompression(dimensionCompression);
    } else if (defaultSpec.dimensionCompression != null) {
      bob.withDimensionCompression(defaultSpec.dimensionCompression);
    } else {
      bob.withDimensionCompression(CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY);
    }

    if (stringDictionaryEncoding != null) {
      bob.withStringDictionaryEncoding(stringDictionaryEncoding);
    } else if (defaultSpec.stringDictionaryEncoding != null) {
      bob.withStringDictionaryEncoding(defaultSpec.stringDictionaryEncoding);
    } else {
      bob.withStringDictionaryEncoding(StringEncodingStrategy.DEFAULT);
    }

    if (metricCompression != null) {
      bob.withMetricCompression(metricCompression);
    } else if (defaultSpec.metricCompression != null) {
      bob.withMetricCompression(defaultSpec.metricCompression);
    } else {
      bob.withMetricCompression(CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY);
    }

    if (longEncoding != null) {
      bob.withLongEncoding(longEncoding);
    } else if (defaultSpec.longEncoding != null) {
      bob.withLongEncoding(defaultSpec.longEncoding);
    } else {
      bob.withLongEncoding(CompressionFactory.DEFAULT_LONG_ENCODING_STRATEGY);
    }

    if (complexMetricCompression != null) {
      bob.withComplexMetricCompression(complexMetricCompression);
    } else if (defaultSpec.complexMetricCompression != null) {
      bob.withComplexMetricCompression(defaultSpec.complexMetricCompression);
    }

    if (jsonCompression != null) {
      bob.withJsonCompression(jsonCompression);
    } else if (defaultSpec.jsonCompression != null) {
      bob.withJsonCompression(defaultSpec.jsonCompression);
    }

    if (segmentLoader != null) {
      bob.withSegmentLoader(segmentLoader);
    } else if (defaultSpec.segmentLoader != null) {
      bob.withSegmentLoader(defaultSpec.segmentLoader);
    }

    if (autoColumnFormatSpec != null) {
      bob.withAutoColumnFormatSpec(autoColumnFormatSpec.getEffectiveSpec(this));
    } else if (defaultSpec.autoColumnFormatSpec != null) {
      bob.withAutoColumnFormatSpec(defaultSpec.autoColumnFormatSpec.getEffectiveSpec(this));
    }


    return bob.build();
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
           Objects.equals(stringDictionaryEncoding, indexSpec.stringDictionaryEncoding) &&
           metricCompression == indexSpec.metricCompression &&
           longEncoding == indexSpec.longEncoding &&
           Objects.equals(complexMetricCompression, indexSpec.complexMetricCompression) &&
           Objects.equals(jsonCompression, indexSpec.jsonCompression) &&
           Objects.equals(segmentLoader, indexSpec.segmentLoader) &&
           Objects.equals(autoColumnFormatSpec, indexSpec.autoColumnFormatSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        bitmapSerdeFactory,
        dimensionCompression,
        stringDictionaryEncoding,
        metricCompression,
        longEncoding,
        complexMetricCompression,
        jsonCompression,
        segmentLoader,
        autoColumnFormatSpec
    );
  }

  @Override
  public String toString()
  {
    return "IndexSpec{" +
           "bitmapSerdeFactory=" + bitmapSerdeFactory +
           ", dimensionCompression=" + dimensionCompression +
           ", stringDictionaryEncoding=" + stringDictionaryEncoding +
           ", metricCompression=" + metricCompression +
           ", longEncoding=" + longEncoding +
           ", complexMetricCompression=" + complexMetricCompression +
           ", jsonCompression=" + jsonCompression +
           ", segmentLoader=" + segmentLoader +
           '}';
  }

  public static class Builder
  {
    @Nullable
    private BitmapSerdeFactory bitmapSerdeFactory;
    @Nullable
    private CompressionStrategy dimensionCompression;
    @Nullable
    private StringEncodingStrategy stringDictionaryEncoding;
    @Nullable
    private CompressionStrategy metricCompression;
    @Nullable
    private CompressionFactory.LongEncodingStrategy longEncoding;
    @Nullable
    private CompressionStrategy complexMetricCompression;
    @Nullable
    private CompressionStrategy jsonCompression;
    @Nullable
    private SegmentizerFactory segmentLoader;
    @Nullable
    private NestedCommonFormatColumnFormatSpec autoColumnFormatSpec;

    public Builder withBitmapSerdeFactory(@Nullable BitmapSerdeFactory bitmapSerdeFactory)
    {
      this.bitmapSerdeFactory = bitmapSerdeFactory;
      return this;
    }

    public Builder withDimensionCompression(@Nullable CompressionStrategy dimensionCompression)
    {
      this.dimensionCompression = dimensionCompression;
      return this;
    }

    public Builder withStringDictionaryEncoding(@Nullable StringEncodingStrategy stringDictionaryEncoding)
    {
      this.stringDictionaryEncoding = stringDictionaryEncoding;
      return this;
    }

    public Builder withMetricCompression(@Nullable CompressionStrategy metricCompression)
    {
      this.metricCompression = metricCompression;
      return this;
    }

    public Builder withComplexMetricCompression(@Nullable CompressionStrategy complexMetricCompression)
    {
      this.complexMetricCompression = complexMetricCompression;
      return this;
    }

    public Builder withLongEncoding(@Nullable CompressionFactory.LongEncodingStrategy longEncoding)
    {
      this.longEncoding = longEncoding;
      return this;
    }

    @Deprecated
    public Builder withJsonCompression(@Nullable CompressionStrategy jsonCompression)
    {
      this.jsonCompression = jsonCompression;
      return this;
    }

    public Builder withSegmentLoader(@Nullable SegmentizerFactory segmentLoader)
    {
      this.segmentLoader = segmentLoader;
      return this;
    }

    public Builder withAutoColumnFormatSpec(@Nullable NestedCommonFormatColumnFormatSpec autoColumnFormatSpec)
    {
      this.autoColumnFormatSpec = autoColumnFormatSpec;
      return this;
    }

    public IndexSpec build()
    {
      return new IndexSpec(
          bitmapSerdeFactory,
          dimensionCompression,
          stringDictionaryEncoding,
          metricCompression,
          longEncoding,
          complexMetricCompression,
          jsonCompression,
          segmentLoader,
          autoColumnFormatSpec
      );
    }
  }
}
