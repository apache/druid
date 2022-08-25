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

package org.apache.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Suppliers;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;

/**
 * A ColumnPartSerde to read and write null-only columns.
 * Its serializer is no-op as nothing is stored for null-only columns.
 * Its deserializer creates necessary column metadata and indexes when the column is read.
 */
public class NullColumnPartSerde implements ColumnPartSerde
{

  private static final Serializer NOOP_SERIALIZER = new Serializer()
  {
    @Override
    public long getSerializedSize()
    {
      return 0;
    }

    @Override
    public void writeTo(WritableByteChannel channel, FileSmoosher smoosher)
    {
    }
  };

  private final int numRows;
  private final BitmapSerdeFactory bitmapSerdeFactory;

  private final NullDictionaryEncodedColumn nullDictionaryEncodedColumn;

  @JsonCreator
  public NullColumnPartSerde(
      @JsonProperty("numRows") int numRows,
      @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory
  )
  {
    this.numRows = numRows;
    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.nullDictionaryEncodedColumn = new NullDictionaryEncodedColumn();
  }

  @JsonProperty
  public int getNumRows()
  {
    return numRows;
  }

  /**
   * This is no longer used for anything, but is required for backwards compatibility, so that segments with
   * explicit null columns can be read with 0.23
   */
  @Deprecated
  @JsonProperty
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  @Nullable
  @Override
  public Serializer getSerializer()
  {
    return NOOP_SERIALIZER;
  }

  @Override
  public Deserializer getDeserializer()
  {
    return (buffer, builder, columnConfig) -> {
      builder.setHasMultipleValues(false)
             .setHasNulls(true)
             .setFilterable(true)
             // this is a bit sneaky, we set supplier to null here to act like a null column instead of a column
             // without any indexes, which is the default state
             .setIndexSupplier(null, true, false)
             .setDictionaryEncodedColumnSupplier(Suppliers.ofInstance(nullDictionaryEncodedColumn));
    };
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
    NullColumnPartSerde partSerde = (NullColumnPartSerde) o;
    return numRows == partSerde.numRows && Objects.equals(bitmapSerdeFactory, partSerde.bitmapSerdeFactory);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(numRows, bitmapSerdeFactory);
  }

  private final class NullDictionaryEncodedColumn implements DictionaryEncodedColumn<String>
  {
    // Get the singleton instance of DimensionSelector.NullDimensionSelectorHolder.NullDimensionSelector
    // to reuse its dictionary lookup logic.
    private final DimensionSelector nullDimensionSelector = DimensionSelector.constant(null);

    @Override
    public int length()
    {
      return numRows;
    }

    @Override
    public boolean hasMultipleValues()
    {
      return false;
    }

    @Override
    public int getSingleValueRow(int rowNum)
    {
      throw new RuntimeException("This method should not be called for null-only columns");
    }

    @Override
    public IndexedInts getMultiValueRow(int rowNum)
    {
      throw new RuntimeException("This method should not be called for null-only columns");
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      return nullDimensionSelector.lookupName(id);
    }

    @Override
    public int lookupId(String name)
    {
      return nullDimensionSelector.idLookup().lookupId(name);
    }

    @Override
    public int getCardinality()
    {
      return 1;
    }

    @Override
    public DimensionSelector makeDimensionSelector(
        ReadableOffset offset,
        @Nullable ExtractionFn extractionFn
    )
    {
      return DimensionSelector.constant(null, extractionFn);
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
        ReadableVectorOffset vectorOffset
    )
    {
      return NilVectorSelector.create(vectorOffset);
    }

    @Override
    public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(
        ReadableVectorOffset vectorOffset
    )
    {
      throw new UnsupportedOperationException("This method should not be called for null-only columns");
    }

    @Override
    public VectorValueSelector makeVectorValueSelector(ReadableVectorOffset offset)
    {
      return NilVectorSelector.create(offset);
    }

    @Override
    public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
    {
      return NilVectorSelector.create(offset);
    }

    @Override
    public void close()
    {
    }
  }
}
