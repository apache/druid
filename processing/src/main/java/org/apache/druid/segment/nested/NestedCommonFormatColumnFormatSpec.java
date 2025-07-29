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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;

import javax.annotation.Nullable;
import java.util.Objects;

public class NestedCommonFormatColumnFormatSpec
{
  public static Builder builder()
  {
    return new Builder();
  }

  public static NestedCommonFormatColumnFormatSpec getEffectiveFormatSpec(
      @Nullable NestedCommonFormatColumnFormatSpec columnFormatSpec,
      IndexSpec indexSpec
  )
  {
    return Objects.requireNonNullElseGet(
        columnFormatSpec,
        () -> NestedCommonFormatColumnFormatSpec.builder()
                                                .setObjectFieldsEncoding(StringEncodingStrategy.UTF8_STRATEGY)
                                                .setObjectStorageEncoding(ObjectStorageEncoding.SMILE)
                                                .build()
    ).getEffectiveSpec(indexSpec);
  }

  @Nullable
  @JsonProperty
  private final StringEncodingStrategy objectFieldsEncoding;
  @Nullable
  @JsonProperty
  private final ObjectStorageEncoding objectStorageEncoding;
  @Nullable
  @JsonProperty
  private final CompressionStrategy objectStorageCompression;
  @Nullable
  @JsonProperty
  private final StringEncodingStrategy stringDictionaryEncoding;
  @Nullable
  @JsonProperty
  private final CompressionStrategy dictionaryEncodedColumnCompression;
  @Nullable
  @JsonProperty
  private final CompressionFactory.LongEncodingStrategy longColumnEncoding;
  @Nullable
  @JsonProperty
  private final CompressionStrategy longColumnCompression;
  @Nullable
  @JsonProperty
  private final CompressionStrategy doubleColumnCompression;
  @Nullable
  @JsonProperty
  private final BitmapSerdeFactory bitmapEncoding;

  @JsonCreator
  public NestedCommonFormatColumnFormatSpec(
      @JsonProperty("objectFieldsEncoding") @Nullable StringEncodingStrategy objectFieldsEncoding,
      @JsonProperty("objectStorageEncoding") @Nullable ObjectStorageEncoding objectStorageEncoding,
      @JsonProperty("objectStorageCompression") @Nullable CompressionStrategy objectStorageCompression,
      @JsonProperty("stringDictionaryEncoding") @Nullable StringEncodingStrategy stringDictionaryEncoding,
      @JsonProperty("dictionaryEncodedColumnCompression") @Nullable CompressionStrategy dictionaryEncodedColumnCompression,
      @JsonProperty("longColumnEncoding") @Nullable CompressionFactory.LongEncodingStrategy longColumnEncoding,
      @JsonProperty("longColumnCompression") @Nullable CompressionStrategy longColumnCompression,
      @JsonProperty("doubleColumnCompression") @Nullable CompressionStrategy doubleColumnCompression,
      @JsonProperty("bitmapEncoding") @Nullable BitmapSerdeFactory bitmapEncoding
  )
  {
    this.objectFieldsEncoding = objectFieldsEncoding;
    this.objectStorageEncoding = objectStorageEncoding;
    this.objectStorageCompression = objectStorageCompression;
    this.stringDictionaryEncoding = stringDictionaryEncoding;
    this.dictionaryEncodedColumnCompression = dictionaryEncodedColumnCompression;
    this.longColumnEncoding = longColumnEncoding;
    this.longColumnCompression = longColumnCompression;
    this.doubleColumnCompression = doubleColumnCompression;
    this.bitmapEncoding = bitmapEncoding;
  }

  public NestedCommonFormatColumnFormatSpec getEffectiveSpec(IndexSpec indexSpec)
  {
    if (bitmapEncoding != null && !bitmapEncoding.equals(indexSpec.getBitmapSerdeFactory())) {
      throw new ISE(
          "bitmapEncoding[%s] does not match indexSpec.bitmap[%s]",
          bitmapEncoding,
          indexSpec.getBitmapSerdeFactory()
      );
    }
    return new NestedCommonFormatColumnFormatSpec(
        objectFieldsEncoding != null ? objectFieldsEncoding : StringEncodingStrategy.DEFAULT,
        objectStorageEncoding != null ? objectStorageEncoding : ObjectStorageEncoding.SMILE,
        objectStorageCompression != null
        ? objectStorageCompression
        : indexSpec.getJsonCompression() != null ? indexSpec.getJsonCompression() : CompressionStrategy.LZ4,
        stringDictionaryEncoding != null ? stringDictionaryEncoding : indexSpec.getStringDictionaryEncoding(),
        dictionaryEncodedColumnCompression != null
        ? dictionaryEncodedColumnCompression
        : indexSpec.getDimensionCompression(),
        longColumnEncoding != null ? longColumnEncoding : indexSpec.getLongEncoding(),
        longColumnCompression != null ? longColumnCompression : indexSpec.getMetricCompression(),
        doubleColumnCompression != null ? doubleColumnCompression : indexSpec.getMetricCompression(),
        bitmapEncoding != null ? bitmapEncoding : indexSpec.getBitmapSerdeFactory()
    );
  }

  @Nullable
  @JsonProperty
  public StringEncodingStrategy getObjectFieldsEncoding()
  {
    return objectFieldsEncoding;
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
  @JsonProperty
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
    return Objects.equals(objectFieldsEncoding, that.objectFieldsEncoding)
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
        objectFieldsEncoding,
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
           "objectKeysEncoding=" + objectFieldsEncoding +
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
    private StringEncodingStrategy objectKeysEncoding;
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

    public Builder setObjectFieldsEncoding(@Nullable StringEncodingStrategy objectKeysEncoding)
    {
      this.objectKeysEncoding = objectKeysEncoding;
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
          objectKeysEncoding,
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
