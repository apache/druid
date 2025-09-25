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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Defines storage format for 'auto' and json columns. This can be convered into the 'effective' format spec by calling
 * {@link #getEffectiveSpec(IndexSpec)}, which will fill in any values which were not specified from
 * {@link IndexSpec#getAutoColumnFormatSpec()}, falling back to hard-coded defaults, useful when generating segments or
 * comparing compaction state.
 */
public class NestedCommonFormatColumnFormatSpec
{
  private static final NestedCommonFormatColumnFormatSpec DEFAULT =
      NestedCommonFormatColumnFormatSpec.builder()
                                        .setObjectFieldsDictionaryEncoding(StringEncodingStrategy.UTF8_STRATEGY)
                                        .setObjectStorageEncoding(ObjectStorageEncoding.SMILE)
                                        .build();

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builder(NestedCommonFormatColumnFormatSpec spec)
  {
    return new Builder(spec);
  }

  public static NestedCommonFormatColumnFormatSpec getEffectiveFormatSpec(
      @Nullable NestedCommonFormatColumnFormatSpec columnFormatSpec,
      IndexSpec indexSpec
  )
  {
    return Objects.requireNonNullElse(columnFormatSpec, DEFAULT).getEffectiveSpec(indexSpec);
  }

  @Nullable
  private final StringEncodingStrategy objectFieldsDictionaryEncoding;
  @Nullable
  private final ObjectStorageEncoding objectStorageEncoding;
  @Nullable
  private final CompressionStrategy objectStorageCompression;
  @Nullable
  private final StringEncodingStrategy stringDictionaryEncoding;
  @Nullable
  private final CompressionStrategy dictionaryEncodedColumnCompression;
  @Nullable
  private final CompressionFactory.LongEncodingStrategy longColumnEncoding;
  @Nullable
  private final CompressionStrategy longColumnCompression;
  @Nullable
  private final CompressionStrategy doubleColumnCompression;
  @Nullable
  private final BitmapSerdeFactory bitmapEncoding;

  @JsonCreator
  public NestedCommonFormatColumnFormatSpec(
      @JsonProperty("objectFieldsDictionaryEncoding") @Nullable StringEncodingStrategy objectFieldsDictionaryEncoding,
      @JsonProperty("objectStorageEncoding") @Nullable ObjectStorageEncoding objectStorageEncoding,
      @JsonProperty("objectStorageCompression") @Nullable CompressionStrategy objectStorageCompression,
      @JsonProperty("stringDictionaryEncoding") @Nullable StringEncodingStrategy stringDictionaryEncoding,
      @JsonProperty("dictionaryEncodedColumnCompression") @Nullable CompressionStrategy dictionaryEncodedColumnCompression,
      @JsonProperty("longColumnEncoding") @Nullable CompressionFactory.LongEncodingStrategy longColumnEncoding,
      @JsonProperty("longColumnCompression") @Nullable CompressionStrategy longColumnCompression,
      @JsonProperty("doubleColumnCompression") @Nullable CompressionStrategy doubleColumnCompression
  )
  {
    this(
        objectFieldsDictionaryEncoding,
        objectStorageEncoding,
        objectStorageCompression,
        stringDictionaryEncoding,
        dictionaryEncodedColumnCompression,
        longColumnEncoding,
        longColumnCompression,
        doubleColumnCompression,
        null
    );
  }

  /**
   * Internal constructor used by {@link Builder} to set {@link #bitmapEncoding} during the process of resolving values
   * for {@link #getEffectiveSpec(IndexSpec)}. {@link #bitmapEncoding} cannot vary per column, and is always set from
   * {@link IndexSpec#getBitmapSerdeFactory()}.
   */
  protected NestedCommonFormatColumnFormatSpec(
      @Nullable StringEncodingStrategy objectFieldsDictionaryEncoding,
      @Nullable ObjectStorageEncoding objectStorageEncoding,
      @Nullable CompressionStrategy objectStorageCompression,
      @Nullable StringEncodingStrategy stringDictionaryEncoding,
      @Nullable CompressionStrategy dictionaryEncodedColumnCompression,
      @Nullable CompressionFactory.LongEncodingStrategy longColumnEncoding,
      @Nullable CompressionStrategy longColumnCompression,
      @Nullable CompressionStrategy doubleColumnCompression,
      @Nullable BitmapSerdeFactory bitmapEncoding
  )
  {
    this.objectFieldsDictionaryEncoding = objectFieldsDictionaryEncoding;
    this.objectStorageEncoding = objectStorageEncoding;
    this.objectStorageCompression = objectStorageCompression;
    this.stringDictionaryEncoding = stringDictionaryEncoding;
    this.dictionaryEncodedColumnCompression = dictionaryEncodedColumnCompression;
    this.longColumnEncoding = longColumnEncoding;
    this.longColumnCompression = longColumnCompression;
    this.doubleColumnCompression = doubleColumnCompression;
    this.bitmapEncoding = bitmapEncoding;
  }

  /**
   * Fully populate all fields of {@link NestedCommonFormatColumnFormatSpec}. Null values are populated first checking
   * {@link IndexSpec#getAutoColumnFormatSpec()}, then falling back to fields on {@link IndexSpec} itself if applicable,
   * and finally resorting to hard coded defaults.
   */
  public NestedCommonFormatColumnFormatSpec getEffectiveSpec(IndexSpec indexSpec)
  {
    // this is a defensive check, the json spec can't set this, only the builder can
    if (bitmapEncoding != null && !bitmapEncoding.equals(indexSpec.getBitmapSerdeFactory())) {
      throw new ISE(
          "bitmapEncoding[%s] does not match indexSpec.bitmap[%s]",
          bitmapEncoding,
          indexSpec.getBitmapSerdeFactory()
      );
    }
    Builder builder = new Builder(this);
    builder.setBitmapEncoding(indexSpec.getBitmapSerdeFactory());

    final NestedCommonFormatColumnFormatSpec defaultSpec;
    if (indexSpec.getAutoColumnFormatSpec() != null) {
      defaultSpec = indexSpec.getAutoColumnFormatSpec();
    } else {
      defaultSpec = DEFAULT;
    }

    if (objectFieldsDictionaryEncoding == null) {
      if (defaultSpec.getObjectFieldsDictionaryEncoding() != null) {
        builder.setObjectFieldsDictionaryEncoding(defaultSpec.getObjectFieldsDictionaryEncoding());
      } else {
        builder.setObjectFieldsDictionaryEncoding(StringEncodingStrategy.DEFAULT);
      }
    }

    if (objectStorageEncoding == null) {
      if (defaultSpec.getObjectStorageEncoding() != null) {
        builder.setObjectStorageEncoding(defaultSpec.getObjectStorageEncoding());
      } else {
        builder.setObjectStorageEncoding(ObjectStorageEncoding.SMILE);
      }
    }

    if (objectStorageCompression == null) {
      if (defaultSpec.getObjectStorageCompression() != null) {
        builder.setObjectStorageCompression(defaultSpec.getObjectStorageCompression());
      } else if (indexSpec.getJsonCompression() != null) {
        builder.setObjectStorageCompression(indexSpec.getJsonCompression());
      } else {
        builder.setObjectStorageCompression(CompressionStrategy.LZ4);
      }
    }

    if (stringDictionaryEncoding == null) {
      if (defaultSpec.getStringDictionaryEncoding() != null) {
        builder.setStringDictionaryEncoding(defaultSpec.getStringDictionaryEncoding());
      } else {
        builder.setStringDictionaryEncoding(indexSpec.getStringDictionaryEncoding());
      }
    }

    if (dictionaryEncodedColumnCompression == null) {
      if (defaultSpec.getDictionaryEncodedColumnCompression() != null) {
        builder.setDictionaryEncodedColumnCompression(defaultSpec.getDictionaryEncodedColumnCompression());
      } else {
        builder.setDictionaryEncodedColumnCompression(indexSpec.getDimensionCompression());
      }
    }

    if (longColumnEncoding == null) {
      if (defaultSpec.getLongColumnEncoding() != null) {
        builder.setLongColumnEncoding(defaultSpec.getLongColumnEncoding());
      } else {
        builder.setLongColumnEncoding(indexSpec.getLongEncoding());
      }
    }

    if (longColumnCompression == null) {
      if (defaultSpec.getLongColumnCompression() != null) {
        builder.setLongColumnCompression(defaultSpec.getLongColumnCompression());
      } else {
        builder.setLongColumnCompression(indexSpec.getMetricCompression());
      }
    }

    if (doubleColumnCompression == null) {
      if (defaultSpec.getDoubleColumnCompression() != null) {
        builder.setDoubleColumnCompression(defaultSpec.getDoubleColumnCompression());
      } else {
        builder.setDoubleColumnCompression(indexSpec.getMetricCompression());
      }
    }

    return builder.build();
  }

  @Nullable
  @JsonProperty
  public StringEncodingStrategy getObjectFieldsDictionaryEncoding()
  {
    return objectFieldsDictionaryEncoding;
  }

  @Nullable
  @JsonProperty
  public ObjectStorageEncoding getObjectStorageEncoding()
  {
    return objectStorageEncoding;
  }

  @Nullable
  @JsonProperty
  public CompressionStrategy getObjectStorageCompression()
  {
    return objectStorageCompression;
  }

  @Nullable
  @JsonProperty
  public StringEncodingStrategy getStringDictionaryEncoding()
  {
    return stringDictionaryEncoding;
  }

  @Nullable
  @JsonProperty
  public CompressionStrategy getDictionaryEncodedColumnCompression()
  {
    return dictionaryEncodedColumnCompression;
  }

  @Nullable
  @JsonProperty
  public CompressionFactory.LongEncodingStrategy getLongColumnEncoding()
  {
    return longColumnEncoding;
  }

  @Nullable
  @JsonProperty
  public CompressionStrategy getLongColumnCompression()
  {
    return longColumnCompression;
  }

  @Nullable
  @JsonProperty
  public CompressionStrategy getDoubleColumnCompression()
  {
    return doubleColumnCompression;
  }

  @Nullable
  @JsonIgnore
  public BitmapSerdeFactory getBitmapEncoding()
  {
    return bitmapEncoding;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NestedCommonFormatColumnFormatSpec that = (NestedCommonFormatColumnFormatSpec) o;
    return Objects.equals(objectFieldsDictionaryEncoding, that.objectFieldsDictionaryEncoding)
           && objectStorageEncoding == that.objectStorageEncoding
           && objectStorageCompression == that.objectStorageCompression
           && Objects.equals(stringDictionaryEncoding, that.stringDictionaryEncoding)
           && dictionaryEncodedColumnCompression == that.dictionaryEncodedColumnCompression
           && longColumnEncoding == that.longColumnEncoding
           && longColumnCompression == that.longColumnCompression
           && doubleColumnCompression == that.doubleColumnCompression
           && Objects.equals(bitmapEncoding, that.bitmapEncoding);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        objectFieldsDictionaryEncoding,
        objectStorageEncoding,
        objectStorageCompression,
        stringDictionaryEncoding,
        dictionaryEncodedColumnCompression,
        longColumnEncoding,
        longColumnCompression,
        doubleColumnCompression,
        bitmapEncoding
    );
  }

  @Override
  public String toString()
  {
    return "NestedCommonFormatColumnFormatSpec{" +
           "objectFieldsDictionaryEncoding=" + objectFieldsDictionaryEncoding +
           ", objectStorageEncoding=" + objectStorageEncoding +
           ", objectStorageCompression=" + objectStorageCompression +
           ", stringDictionaryEncoding=" + stringDictionaryEncoding +
           ", dictionaryEncodedColumnCompression=" + dictionaryEncodedColumnCompression +
           ", longColumnEncoding=" + longColumnEncoding +
           ", longColumnCompression=" + longColumnCompression +
           ", doubleColumnCompression=" + doubleColumnCompression +
           ", bitmapEncoding=" + bitmapEncoding +
           '}';
  }

  public static class Builder
  {
    @Nullable
    private StringEncodingStrategy objectFieldsDictionaryEncoding;
    @Nullable
    private ObjectStorageEncoding objectStorageEncoding;
    @Nullable
    private CompressionStrategy objectStorageCompression;
    @Nullable
    private StringEncodingStrategy stringDictionaryEncoding;
    @Nullable
    private CompressionStrategy dictionaryEncodedColumnCompression;
    @Nullable
    private CompressionFactory.LongEncodingStrategy longColumnEncoding;
    @Nullable
    private CompressionStrategy longColumnCompression;
    @Nullable
    private CompressionStrategy doubleColumnCompression;
    @Nullable
    private BitmapSerdeFactory bitmapEncoding;

    public Builder()
    {

    }

    public Builder(NestedCommonFormatColumnFormatSpec spec)
    {
      this.objectFieldsDictionaryEncoding = spec.objectFieldsDictionaryEncoding;
      this.objectStorageEncoding = spec.objectStorageEncoding;
      this.objectStorageCompression = spec.objectStorageCompression;
      this.stringDictionaryEncoding = spec.stringDictionaryEncoding;
      this.dictionaryEncodedColumnCompression = spec.dictionaryEncodedColumnCompression;
      this.longColumnEncoding = spec.longColumnEncoding;
      this.longColumnCompression = spec.longColumnCompression;
      this.doubleColumnCompression = spec.doubleColumnCompression;
      this.bitmapEncoding = spec.bitmapEncoding;
    }

    public Builder setObjectFieldsDictionaryEncoding(@Nullable StringEncodingStrategy objectFieldsDictionaryEncoding)
    {
      this.objectFieldsDictionaryEncoding = objectFieldsDictionaryEncoding;
      return this;
    }

    public Builder setObjectStorageEncoding(@Nullable ObjectStorageEncoding objectStorageEncoding)
    {
      this.objectStorageEncoding = objectStorageEncoding;
      return this;
    }

    public Builder setObjectStorageCompression(@Nullable CompressionStrategy objectStorageCompression)
    {
      this.objectStorageCompression = objectStorageCompression;
      return this;
    }

    public Builder setStringDictionaryEncoding(@Nullable StringEncodingStrategy stringDictionaryEncoding)
    {
      this.stringDictionaryEncoding = stringDictionaryEncoding;
      return this;
    }

    public Builder setDictionaryEncodedColumnCompression(
        @Nullable CompressionStrategy dictionaryEncodedColumnCompression
    )
    {
      this.dictionaryEncodedColumnCompression = dictionaryEncodedColumnCompression;
      return this;
    }

    public Builder setLongColumnEncoding(@Nullable CompressionFactory.LongEncodingStrategy longColumnEncoding)
    {
      this.longColumnEncoding = longColumnEncoding;
      return this;
    }

    public Builder setLongColumnCompression(@Nullable CompressionStrategy longColumnCompression)
    {
      this.longColumnCompression = longColumnCompression;
      return this;
    }

    public Builder setDoubleColumnCompression(@Nullable CompressionStrategy doubleColumnCompression)
    {
      this.doubleColumnCompression = doubleColumnCompression;
      return this;
    }

    public Builder setBitmapEncoding(@Nullable BitmapSerdeFactory bitmapEncoding)
    {
      this.bitmapEncoding = bitmapEncoding;
      return this;
    }

    public NestedCommonFormatColumnFormatSpec build()
    {
      return new NestedCommonFormatColumnFormatSpec(
          objectFieldsDictionaryEncoding,
          objectStorageEncoding,
          objectStorageCompression,
          stringDictionaryEncoding,
          dictionaryEncodedColumnCompression,
          longColumnEncoding,
          longColumnCompression,
          doubleColumnCompression,
          bitmapEncoding
      );
    }
  }
}
