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

package org.apache.druid.mapStringString;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.column.StringDictionaryEncodedColumn;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.ZeroIndexedInts;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.historical.SingleValueHistoricalDimensionSelector;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class MapStringStringComplexColumn implements ComplexColumn
{
  private static final NullDimensionSelector NULL_DIMENSION_SELECTOR = new NullDimensionSelector();

  private final MapStringStringColumnMetadata metadata;
  private final GenericIndexed<String> keys;
  private final ColumnConfig columnConfig;
  private final SmooshedFileMapper fileMapper;

  private final ConcurrentHashMap<String, ColumnHolder> columns = new ConcurrentHashMap<>();

  public MapStringStringComplexColumn(
      MapStringStringColumnMetadata metadata,
      GenericIndexed<String> keys,
      ColumnConfig columnConfig,
      SmooshedFileMapper fileMapper
  )
  {
    this.metadata = metadata;
    this.keys = keys;
    this.columnConfig = columnConfig;
    this.fileMapper = fileMapper;
  }

  @Override
  public Class<?> getClazz()
  {
    return MapStringStringRow.class;
  }

  @Override
  public String getTypeName()
  {
    return MapStringStringDruidModule.TYPE_NAME;
  }

  @Override
  public Object getRowValue(int rowNum)
  {
    MapStringStringRow.Builder builder = new MapStringStringRow.Builder();
    keys.forEach(
        key -> {
          StringDictionaryEncodedColumn col = (StringDictionaryEncodedColumn) getColumn(key).getColumn();
          String value = col.lookupName(col.getSingleValueRow(rowNum));
          builder.put(key, value);
        }
    );
    return builder.build();
  }

  @Override
  public int getLength()
  {
    //This is used by SegmentMetadata query, left out with dummy impl for now.
    return 0;
  }

  @Override
  public void close()
  {

  }

  public DimensionSelector makeDimensionSelector(String key, ReadableOffset readableOffset, ExtractionFn fn)
  {
    Preconditions.checkNotNull(key, "Null key");

    if (keys.indexOf(key) >= 0) {
      StringDictionaryEncodedColumn col = (StringDictionaryEncodedColumn) getColumn(key).getColumn();
      return col.makeDimensionSelector(readableOffset, fn);
    } else {
      return NULL_DIMENSION_SELECTOR;
    }
  }

  public ColumnValueSelector<?> makeColumnValueSelector(String key, ReadableOffset readableOffset)
  {
    Preconditions.checkNotNull(key, "Null key");

    if (keys.indexOf(key) >= 0) {
      StringDictionaryEncodedColumn col = (StringDictionaryEncodedColumn) getColumn(key).getColumn();
      return col.makeColumnValueSelector(readableOffset);
    } else {
      return NULL_DIMENSION_SELECTOR;
    }
  }

  public BitmapIndex makeBitmapIndex(String key)
  {
    return getColumn(key).getBitmapIndex();
  }

  private ColumnHolder getColumn(String key)
  {
    return columns.computeIfAbsent(
        key,
        this::readStringDictionaryEncodedColumn
    );
  }

  private ColumnHolder readStringDictionaryEncodedColumn(String key)
  {
    try {
      ByteBuffer dataBuffer = fileMapper.mapFile(MapStringStringColumnSerializer.getKeyColumnFileName(key, metadata.getFileNameBase()));
      if (dataBuffer == null) {
        throw new ISE("WTH! Can't find [%s] key data in [%s] file.", key, metadata.getFileNameBase());
      }

      DictionaryEncodedColumnPartSerde serde = DictionaryEncodedColumnPartSerde.createDeserializer(
          metadata.getBitmapSerdeFactory(),
          metadata.getByteOrder()
      );
      ColumnBuilder columnBuilder = new ColumnBuilder().setFileMapper(fileMapper);
      serde.getDeserializer().read(dataBuffer, columnBuilder, columnConfig);
      return columnBuilder.build();
    }
    catch (IOException ex) {
      throw new RE(ex, "IO error while loading data for [%s]", key);
    }
  }

  private static class NullDimensionSelector implements SingleValueHistoricalDimensionSelector, IdLookup
  {
    private NullDimensionSelector()
    {
      // Singleton.
    }

    @Override
    public IndexedInts getRow()
    {
      return ZeroIndexedInts.instance();
    }

    @Override
    public int getRowValue(int offset)
    {
      return 0;
    }

    @Override
    public IndexedInts getRow(int offset)
    {
      return getRow();
    }

    @Override
    public ValueMatcher makeValueMatcher(@Nullable String value)
    {
      return BooleanValueMatcher.of(value == null);
    }

    @Override
    public ValueMatcher makeValueMatcher(Predicate<String> predicate)
    {
      return BooleanValueMatcher.of(predicate.apply(null));
    }

    @Override
    public int getValueCardinality()
    {
      return 1;
    }

    @Override
    @Nullable
    public String lookupName(int id)
    {
      return null;
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return true;
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return this;
    }

    @Override
    public int lookupId(@Nullable String name)
    {
      return NullHandling.isNullOrEquivalent(name) ? 0 : -1;
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return null;
    }

    @Override
    public Class classOfObject()
    {
      return Object.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // nothing to inspect
    }
  }
}
