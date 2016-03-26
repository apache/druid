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
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.spatial.search.Bound;
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
import io.druid.segment.serde.FloatGenericColumnPartSerde;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class FloatDimensionHandler implements DimensionHandler<Float, Float>
{
  private static final Logger log = new Logger(FloatDimensionHandler.class);

  public static final Function<Object, Float> FLOAT_TRANSFORMER = new Function<Object, Float>()
  {
    @Override
    public Float apply(final Object o)
    {
      if (o == null) {
        return null;
      }

      if (o instanceof Float) {
        return (Float) o;
      }

      if (o instanceof String) {
        if (((String) o).length() == 0) {
          return 0.0f;
        }
        try {
          return Float.valueOf((String) o);
        }
        catch (NumberFormatException nfe) {
          throw new ParseException(nfe, "Unable to parse value[%s] as float in column: ", o);
        }
      }
      if (o instanceof Number) {
        return ((Number) o).floatValue();
      }
      return null;
    }
  };

  private static final Comparator<Float> Float_COMPARATOR = new Comparator<Float>()
  {
    @Override
    public int compare(Float o1, Float o2)
    {
      if (o1 == null) {
        return o2 == null ? 0 : -1;
      }
      if (o2 == null) {
        return 1;
      }
      return Float.compare(o1, o2);
    }
  };

  private final String dimensionName;

  public FloatDimensionHandler(String dimensionName)
  {
    this.dimensionName = dimensionName;
  }

  @Override
  public DimensionSelector getDimensionSelector(
      ColumnSelectorFactory cursor, DimensionSpec dimSpec, ColumnCapabilities capabilities
  )
  {
    return new FloatDimensionSelector(
        cursor.makeFloatColumnSelector(dimensionName),
        dimSpec.getExtractionFn(),
        capabilities
    );
  }

  @Override
  public Function<Object, Float> getValueTypeTransformer()
  {
    return FLOAT_TRANSFORMER;
  }

  @Override
  public Comparator<Float> getEncodedComparator()
  {
    return Float_COMPARATOR;
  }

  @Override
  public Comparator<Float> getActualComparator()
  {
    return Float_COMPARATOR;
  }

  @Override
  public byte[] getBytesFromRowValue(Float rowVal)
  {
    return Ints.toByteArray(Float.floatToIntBits(rowVal));
  }

  @Override
  public Float getRowValueFromBytes(byte[] bytes)
  {
    return Float.intBitsToFloat(Ints.fromByteArray(bytes));
  }

  @Override
  public int getEncodedValueSize()
  {
    return Floats.BYTES;
  }

  @Override
  public DimensionIndexer makeIndexer(Object lock)
  {
    return new FloatDimensionIndexer();
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
    return new FloatDimensionMerger(indexSpec, outDir, ioPeon, v9Smoosher, mapper, capabilities, progress);
  }

  @Override
  public DimensionColumnReader makeColumnReader(Column column)
  {
    return new FloatDimensionColumnReader(column);
  }


  public class FloatDimensionIndexer implements DimensionIndexer<Float, Float>
  {
    @Override
    public Comparable[] processRowValsToIndexKey(Object dimValues)
    {
      final Comparable[] dimensionValues;
      Float transformedVal = 0.0f;
      try {
        if (dimValues == null) {
          return null;
        } else if (dimValues instanceof List) {
          List<Object> dimValuesList = (List) dimValues;

          if (dimValuesList.size() > 1) {
            throw new UnsupportedOperationException("Multi-value rows are not supported on Float dimensions.");
          }

          dimensionValues = new Comparable[dimValuesList.size()];
          for (int i = 0; i < dimValuesList.size(); i++) {
            transformedVal = FLOAT_TRANSFORMER.apply(dimValuesList.get(i));
            dimensionValues[i] = transformedVal;
          }
        } else {
          transformedVal = FLOAT_TRANSFORMER.apply(dimValues);
          dimensionValues = new Comparable[]{transformedVal};
        }
      }
      catch (ParseException pe) {
        throw new ParseException(pe.getMessage() + dimensionName);
      }
      return dimensionValues;
    }

    @Override
    public Float getActualValue(Float intermediateValue, boolean idSorted)
    {
      return intermediateValue;
    }

    @Override
    public Float getEncodedValue(Float fullValue, boolean idSorted)
    {
      return fullValue;
    }

    @Override
    public Float getSortedEncodedValueFromUnsorted(Float unsortedIntermediateValue)
    {
      return unsortedIntermediateValue;
    }

    @Override
    public Float getUnsortedEncodedValueFromSorted(Float sortedIntermediateValue)
    {
      return sortedIntermediateValue;
    }

    @Override
    public Indexed<Float> getSortedIndexedValues()
    {
      throw new UnsupportedOperationException("getSortedIndexedValues() is not supported for Float column: "
                                              + dimensionName);
    }

    @Override
    public Float getMinValue()
    {
      return null;
    }

    @Override
    public Float getMaxValue()
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

  public class FloatDimensionMerger implements DimensionMerger<Float, Float>
  {
    private final IndexSpec indexSpec;
    private final File outDir;
    private final IOPeon ioPeon;
    private final FileSmoosher v9Smoosher;
    private final ObjectMapper mapper;
    private final ColumnCapabilities capabilities;
    private boolean useV8 = false;
    private ProgressIndicator progress;

    private GenericColumnSerializer valueWriter;

    private MetricColumnSerializer valueWriterV8;

    public FloatDimensionMerger(
        IndexSpec indexSpec,
        File outDir,
        IOPeon ioPeon,
        FileSmoosher v9Smoosher,
        ObjectMapper mapper,
        ColumnCapabilities capabilities,
        ProgressIndicator progress
    )
    {
      this.indexSpec = indexSpec;
      this.ioPeon = ioPeon;
      this.outDir = outDir;
      this.v9Smoosher = v9Smoosher;
      this.mapper = mapper;
      this.capabilities = capabilities;
      this.progress = progress;
    }

    @Override
    public void initWriters() throws IOException
    {
      if (useV8) {
        valueWriterV8 = new FloatMetricColumnSerializer(dimensionName, outDir, ioPeon);
        valueWriterV8.open();
      } else {
        String filenameBase = String.format("%s.forward_dim", dimensionName);
        valueWriter = FloatColumnSerializer.create(ioPeon, filenameBase, indexSpec.getDimensionCompressionStrategy());
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
      float startTime = System.currentTimeMillis();

      valueWriter.close();

      final ColumnDescriptor.Builder builder = ColumnDescriptor.builder()
                                                               .setValueType(ValueType.FLOAT);

      builder.addSerde(
          FloatGenericColumnPartSerde.serializerBuilder()
                                     .withByteOrder(IndexIO.BYTE_ORDER)
                                     .withDelegate((FloatColumnSerializer) valueWriter)
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
    public Float getConvertedEncodedValue(Float value, int indexNumber)
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

  public class FloatDimensionColumnReader implements DimensionColumnReader<Float, Float>
  {
    private final Column column;
    private final GenericColumn genericColumn;

    public FloatDimensionColumnReader(Column column)
    {
      this.column = column;
      this.genericColumn = column.getGenericColumn();
    }

    @Override
    public ImmutableBitmap getBitmapIndex(
        String value, BitmapFactory bitmapFactory, int numRows
    )
    {
      if (column == null) {
        return bitmapFactory.makeEmptyImmutableBitmap();
      }
      final float parsedFloat;
      try {
        parsedFloat = Float.parseFloat(value);
      }
      catch (NumberFormatException nfe) {
        throw new ParseException("Could not parse value[%s] as float!", value);
      }

      final MutableBitmap bitmap = bitmapFactory.makeEmptyMutableBitmap();
      for (int rowIdx = 0; rowIdx < column.getLength(); rowIdx++) {
        float rowVal = genericColumn.getFloatSingleValueRow(rowIdx);
        if (rowVal == parsedFloat) {
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

      Float floatVal;
      for (int rowIdx = 0; rowIdx < column.getLength(); rowIdx++) {
        floatVal = genericColumn.getFloatSingleValueRow(rowIdx);
        if (predicate.apply(floatVal)) {
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
      throw new UnsupportedOperationException("Float column does not support spatial filtering: " + dimensionName);
    }

    @Override
    public Float getMinValue()
    {
      return null;
    }

    @Override
    public Float getMaxValue()
    {
      return null;
    }

    @Override
    public Indexed<Float> getSortedIndexedValues()
    {
      throw new UnsupportedOperationException("getSortedIndexedValues() is not supported for Float column: "
                                              + dimensionName);
    }

    @Override
    public ColumnCapabilities getCapabilities()
    {
      return column.getCapabilities();
    }

  }


}
