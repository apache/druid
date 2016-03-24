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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Longs;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.spatial.search.Bound;
import com.metamx.common.IAE;
import com.metamx.common.io.smoosh.FileSmoosher;
import com.metamx.common.io.smoosh.SmooshedWriter;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.common.utils.SerializerUtils;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.Indexed;
import io.druid.segment.serde.LongGenericColumnPartSerde;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class LongDimensionHandler implements DimensionHandler<Long, Long>
{
  private static final Logger log = new Logger(LongDimensionHandler.class);

  public static final Function<Object, Long> LONG_TRANSFORMER = new Function<Object, Long>()
  {
    @Override
    public Long apply(final Object o)
    {
      if (o == null) {
        return null;
      }

      if (o instanceof Long) {
        return (Long) o;
      }

      if (o instanceof String) {
        if (((String) o).length() == 0) {
          return 0L;
        }
        try {
          return Long.valueOf((String) o);
        }
        catch (NumberFormatException nfe) {
          throw new ParseException(nfe, "Unable to parse value[%s] as long in column: ", o);
        }
      }
      if (o instanceof Number) {
        return ((Number) o).longValue();
      }
      return null;
    }
  };

  private static final Comparator<Long> LONG_COMPARATOR = new Comparator<Long>()
  {
    @Override
    public int compare(Long o1, Long o2)
    {
      if (o1 == null) {
        return o2 == null ? 0 : -1;
      }
      if (o2 == null) {
        return 1;
      }
      return Long.compare(o1, o2);
    }
  };

  private final String dimensionName;

  public LongDimensionHandler(String dimensionName)
  {
    this.dimensionName = dimensionName;
  }

  @Override
  public DimensionSelector getDimensionSelector(
      ColumnSelectorFactory cursor, DimensionSpec dimSpec, ColumnCapabilities capabilities
  )
  {
    return new LongDimensionSelector(
        cursor.makeLongColumnSelector(dimensionName),
        dimSpec.getExtractionFn(),
        capabilities
    );
  }

  @Override
  public Function<Object, Long> getValueTypeTransformer()
  {
    return LONG_TRANSFORMER;
  }

  @Override
  public Comparator<Long> getEncodedComparator()
  {
    return LONG_COMPARATOR;
  }

  @Override
  public Comparator<Long> getActualComparator()
  {
    return LONG_COMPARATOR;
  }

  @Override
  public byte[] getBytesFromRowValue(Long rowVal)
  {
    return Longs.toByteArray(rowVal);
  }

  @Override
  public Long getRowValueFromBytes(byte[] bytes)
  {
    return Longs.fromByteArray(bytes);
  }

  @Override
  public int getEncodedValueSize()
  {
    return Longs.BYTES;
  }

  @Override
  public DimensionIndexer makeIndexer(Object lock)
  {
    return new LongDimensionIndexer();
  }

  @Override
  public DimensionMerger makeMerger(
      IndexSpec indexSpec,
      File outDir,
      IOPeon ioPeon,
      FileSmoosher v9Smoosher,
      ObjectMapper mapper,
      ColumnCapabilities capabilities,
      ProgressIndicator progress
  )
  {
    return new LongDimensionMerger(indexSpec, outDir, ioPeon, v9Smoosher, mapper, capabilities);
  }

  @Override
  public DimensionColumnReader makeColumnReader(Column column)
  {
    return new LongDimensionColumnReader(column);
  }


  public class LongDimensionIndexer implements DimensionIndexer<Long, Long>
  {
    @Override
    public Comparable[] processRowValsToIndexKey(Object dimValues)
    {
      final Comparable[] dimensionValues;
      Long transformedVal = 0L;
      try {
        if (dimValues == null) {
          return null;
        } else if (dimValues instanceof List) {
          List<Object> dimValuesList = (List) dimValues;

          if (dimValuesList.size() > 1) {
            throw new UnsupportedOperationException("Multi-value rows are not supported on Long dimensions.");
          }

          dimensionValues = new Comparable[dimValuesList.size()];
          for (int i = 0; i < dimValuesList.size(); i++) {
            transformedVal = LONG_TRANSFORMER.apply(dimValuesList.get(i));
            dimensionValues[i] = transformedVal;
          }
        } else {
          transformedVal = LONG_TRANSFORMER.apply(dimValues);
          dimensionValues = new Comparable[]{transformedVal};
        }
      }
      catch (ParseException pe) {
        throw new ParseException(pe.getMessage() + dimensionName);
      }
      return dimensionValues;
    }

    @Override
    public Long getActualValue(Long intermediateValue, boolean idSorted)
    {
      return intermediateValue;
    }

    @Override
    public Long getEncodedValue(Long fullValue, boolean idSorted)
    {
      return fullValue;
    }

    @Override
    public Long getSortedEncodedValueFromUnsorted(Long unsortedIntermediateValue)
    {
      return unsortedIntermediateValue;
    }

    @Override
    public Long getUnsortedEncodedValueFromSorted(Long sortedIntermediateValue)
    {
      return sortedIntermediateValue;
    }

    @Override
    public Indexed<Long> getSortedIndexedValues()
    {
      throw new UnsupportedOperationException("getSortedIndexedValues() is not supported for Long column: "
                                              + dimensionName);
    }

    @Override
    public Long getMinValue()
    {
      return null;
    }

    @Override
    public Long getMaxValue()
    {
      return null;
    }

    @Override
    public void addNullLookup()
    {
    }

    @Override
    public int getCardinality()
    {
      // DimensionSelector.getValueCardinality() returns Integer.MAX_VALUE for Long and Floats.
      // Return the same here for consistency.
      return Integer.MAX_VALUE;
    }
  }

  public class LongDimensionMerger implements DimensionMerger<Long, Long>
  {
    private final IndexSpec indexSpec;
    private final File outDir;
    private final IOPeon ioPeon;
    private final FileSmoosher v9Smoosher;
    private final ObjectMapper mapper;
    private final ColumnCapabilities capabilities;

    private boolean useV8 = false;

    private GenericColumnSerializer valueWriter;

    private MetricColumnSerializer valueWriterV8;

    public LongDimensionMerger(
        IndexSpec indexSpec,
        File outDir,
        IOPeon ioPeon,
        FileSmoosher v9Smoosher,
        ObjectMapper mapper,
        ColumnCapabilities capabilities
    )
    {
      this.indexSpec = indexSpec;
      this.ioPeon = ioPeon;
      this.outDir = outDir;
      this.v9Smoosher = v9Smoosher;
      this.mapper = mapper;
      this.capabilities = capabilities;
    }


    @Override
    public void initWriters() throws IOException
    {
      if (useV8) {
        valueWriterV8 = new LongMetricColumnSerializer(dimensionName, outDir, ioPeon);
        valueWriterV8.open();
      } else {
        String filenameBase = String.format("%s.forward_dim", dimensionName);
        valueWriter = LongColumnSerializer.create(ioPeon, filenameBase, indexSpec.getDimensionCompressionStrategy());
        valueWriter.open();
      }
    }

    @Override
    public void mergeAcrossSegments(List<IndexableAdapter> adapters) throws IOException
    {
      return;
    }

    @Override
    public void addColumnValue(Comparable[] values) throws IOException
    {
      // Only singleValue numerics are supported right now.
      if (useV8) {
        valueWriterV8.serialize(values[0]);
      } else {
        valueWriter.serialize(values[0]);
      }
    }

    @Override
    public void closeWriters(List<IntBuffer> rowNumConversions) throws IOException
    {
      long startTime = System.currentTimeMillis();

      valueWriter.close();

      final ColumnDescriptor.Builder builder = ColumnDescriptor.builder()
                                                               .setValueType(ValueType.LONG);

      builder.addSerde(
          LongGenericColumnPartSerde.serializerBuilder()
                                    .withByteOrder(IndexIO.BYTE_ORDER)
                                    .withDelegate((LongColumnSerializer) valueWriter)
                                    .build());
      final ColumnDescriptor serdeficator = builder.build();

      makeColumn(v9Smoosher, dimensionName, serdeficator);

      log.info("Completed dimension column[%s] in %,d millis.", dimensionName, System.currentTimeMillis() - startTime);
    }

    @Override
    public boolean needsConvertMissingValues()
    {
      return true;
    }

    @Override
    public boolean canSkip()
    {
      return false;
    }

    @Override
    public Long getConvertedEncodedValue(Long value, int indexNumber)
    {
      return value;
    }

    @Override
    public void setWriteV8()
    {
      useV8 = true;
    }

    @Override
    public void closeWritersV8(
        List<IntBuffer> rowNumConversions,
        OutputSupplier<FileOutputStream> invertedOut,
        OutputSupplier<FileOutputStream> spatialOut
    ) throws IOException
    {
      valueWriterV8.closeFile(
          IndexIO.makeNumericDimFile(outDir, dimensionName, IndexIO.BYTE_ORDER)
      );
    }

    private void makeColumn(
        final FileSmoosher v9Smoosher,
        final String columnName,
        final ColumnDescriptor serdeficator
    ) throws IOException
    {
      final SerializerUtils serializerUtils = new SerializerUtils();

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      serializerUtils.writeString(baos, mapper.writeValueAsString(serdeficator));
      byte[] specBytes = baos.toByteArray();

      final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
          columnName, serdeficator.numBytes() + specBytes.length
      );
      try {
        channel.write(ByteBuffer.wrap(specBytes));
        serdeficator.write(channel);
      }
      finally {
        channel.close();
      }
    }
  }

  public class LongDimensionColumnReader implements DimensionColumnReader<Long, Long>
  {
    private final Column column;
    private final GenericColumn genericColumn;

    public LongDimensionColumnReader(Column column)
    {
      this.column = column;
      this.genericColumn = column == null ? null : column.getGenericColumn();
    }

    @Override
    public ImmutableBitmap getBitmapIndex(
        String value, BitmapFactory bitmapFactory, int numRows
    )
    {
      if (column == null) {
        return bitmapFactory.makeEmptyImmutableBitmap();
      }
      final long parsedLong;
      try {
        parsedLong = Long.parseLong(value);
      }
      catch (NumberFormatException nfe) {
        throw new ParseException("Could not parse value[%s] as long!", value);
      }

      final MutableBitmap bitmap = bitmapFactory.makeEmptyMutableBitmap();
      for (int rowIdx = 0; rowIdx < column.getLength(); rowIdx++) {
        long rowVal = genericColumn.getLongSingleValueRow(rowIdx);
        if (rowVal == parsedLong) {
          bitmap.add(rowIdx);
        }
      }

      return bitmapFactory.makeImmutableBitmap(bitmap);
    }

    @Override
    public ImmutableBitmap getBitmapIndex(
        Predicate predicate, BitmapFactory bitmapFactory
    )
    {
      if (column == null) {
        return bitmapFactory.makeEmptyImmutableBitmap();
      }

      final MutableBitmap bitmap = bitmapFactory.makeEmptyMutableBitmap();

      Long longVal;
      for (int rowIdx = 0; rowIdx < column.getLength(); rowIdx++) {
        longVal = genericColumn.getLongSingleValueRow(rowIdx);
        if (predicate.apply(longVal)) {
          bitmap.add(rowIdx);
        }
      }
      return bitmapFactory.makeImmutableBitmap(bitmap);
    }

    @Override
    public ImmutableBitmap getBitmapIndex(
        Bound bound, BitmapFactory bitmapFactory
    )
    {
      throw new UnsupportedOperationException("Long column does not support spatial filtering: " + dimensionName);
    }

    @Override
    public Long getMinValue()
    {
      return null;
    }

    @Override
    public Long getMaxValue()
    {
      return null;
    }

    @Override
    public Indexed<Long> getSortedIndexedValues()
    {
      throw new UnsupportedOperationException("getSortedIndexedValues() is not supported for Long column: "
                                              + dimensionName);
    }

    @Override
    public ColumnCapabilities getCapabilities()
    {
      return column.getCapabilities();
    }

  }


}
