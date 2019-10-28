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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.spatial.ImmutableRTree;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.Smoosh;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerde;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CompressedColumnarLongsSupplier;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ImmutableRTreeObjectStrategy;
import org.apache.druid.segment.data.IndexedIterable;
import org.apache.druid.segment.data.VSizeColumnarMultiInts;
import org.apache.druid.segment.serde.BitmapIndexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.DictionaryEncodedColumnSupplier;
import org.apache.druid.segment.serde.FloatNumericColumnSupplier;
import org.apache.druid.segment.serde.LongNumericColumnSupplier;
import org.apache.druid.segment.serde.SpatialIndexColumnPartSupplier;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class IndexIO
{
  public static final byte V8_VERSION = 0x8;
  public static final byte V9_VERSION = 0x9;
  public static final int CURRENT_VERSION_ID = V9_VERSION;
  public static final BitmapSerdeFactory LEGACY_FACTORY = new BitmapSerde.LegacyBitmapSerdeFactory();

  public static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();

  private final Map<Integer, IndexLoader> indexLoaders;

  private static final EmittingLogger log = new EmittingLogger(IndexIO.class);
  private static final SerializerUtils SERIALIZER_UTILS = new SerializerUtils();

  private final ObjectMapper mapper;

  @Inject
  public IndexIO(ObjectMapper mapper, ColumnConfig columnConfig)
  {
    this.mapper = Preconditions.checkNotNull(mapper, "null ObjectMapper");
    Preconditions.checkNotNull(columnConfig, "null ColumnConfig");
    ImmutableMap.Builder<Integer, IndexLoader> indexLoadersBuilder = ImmutableMap.builder();
    LegacyIndexLoader legacyIndexLoader = new LegacyIndexLoader(new DefaultIndexIOHandler(), columnConfig);
    for (int i = 0; i <= V8_VERSION; i++) {
      indexLoadersBuilder.put(i, legacyIndexLoader);
    }
    indexLoadersBuilder.put((int) V9_VERSION, new V9IndexLoader(columnConfig));
    indexLoaders = indexLoadersBuilder.build();
  }

  public void validateTwoSegments(File dir1, File dir2) throws IOException
  {
    try (QueryableIndex queryableIndex1 = loadIndex(dir1)) {
      try (QueryableIndex queryableIndex2 = loadIndex(dir2)) {
        validateTwoSegments(
            new QueryableIndexIndexableAdapter(queryableIndex1),
            new QueryableIndexIndexableAdapter(queryableIndex2)
        );
      }
    }
  }

  public void validateTwoSegments(final IndexableAdapter adapter1, final IndexableAdapter adapter2)
  {
    if (adapter1.getNumRows() != adapter2.getNumRows()) {
      throw new SegmentValidationException(
          "Row count mismatch. Expected [%d] found [%d]",
          adapter1.getNumRows(),
          adapter2.getNumRows()
      );
    }
    {
      final Set<String> dimNames1 = Sets.newHashSet(adapter1.getDimensionNames());
      final Set<String> dimNames2 = Sets.newHashSet(adapter2.getDimensionNames());
      if (!dimNames1.equals(dimNames2)) {
        throw new SegmentValidationException(
            "Dimension names differ. Expected [%s] found [%s]",
            dimNames1,
            dimNames2
        );
      }
      final Set<String> metNames1 = Sets.newHashSet(adapter1.getMetricNames());
      final Set<String> metNames2 = Sets.newHashSet(adapter2.getMetricNames());
      if (!metNames1.equals(metNames2)) {
        throw new SegmentValidationException("Metric names differ. Expected [%s] found [%s]", metNames1, metNames2);
      }
    }
    final RowIterator it1 = adapter1.getRows();
    final RowIterator it2 = adapter2.getRows();
    long row = 0L;
    while (it1.moveToNext()) {
      if (!it2.moveToNext()) {
        throw new SegmentValidationException("Unexpected end of second adapter");
      }
      final RowPointer rp1 = it1.getPointer();
      final RowPointer rp2 = it2.getPointer();
      ++row;
      if (rp1.getRowNum() != rp2.getRowNum()) {
        throw new SegmentValidationException("Row number mismatch: [%d] vs [%d]", rp1.getRowNum(), rp2.getRowNum());
      }
      try {
        validateRowValues(rp1, adapter1, rp2, adapter2);
      }
      catch (SegmentValidationException ex) {
        throw new SegmentValidationException(ex, "Validation failure on row %d: [%s] vs [%s]", row, rp1, rp2);
      }
    }
    if (it2.moveToNext()) {
      throw new SegmentValidationException("Unexpected end of first adapter");
    }
    if (row != adapter1.getNumRows()) {
      throw new SegmentValidationException(
          "Actual Row count mismatch. Expected [%d] found [%d]",
          row,
          adapter1.getNumRows()
      );
    }
  }

  public QueryableIndex loadIndex(File inDir) throws IOException
  {
    final int version = SegmentUtils.getVersionFromDir(inDir);

    final IndexLoader loader = indexLoaders.get(version);

    if (loader != null) {
      return loader.load(inDir, mapper);
    } else {
      throw new ISE("Unknown index version[%s]", version);
    }
  }

  public static void checkFileSize(File indexFile) throws IOException
  {
    final long fileSize = indexFile.length();
    if (fileSize > Integer.MAX_VALUE) {
      throw new IOE("File[%s] too large[%d]", indexFile, fileSize);
    }
  }

  interface IndexIOHandler
  {
    MMappedIndex mapDir(File inDir) throws IOException;
  }

  private static void validateRowValues(
      RowPointer rp1,
      IndexableAdapter adapter1,
      RowPointer rp2,
      IndexableAdapter adapter2
  )
  {
    if (rp1.getTimestamp() != rp2.getTimestamp()) {
      throw new SegmentValidationException(
          "Timestamp mismatch. Expected %d found %d",
          rp1.getTimestamp(),
          rp2.getTimestamp()
      );
    }
    final List<Object> dims1 = rp1.getDimensionValuesForDebug();
    final List<Object> dims2 = rp2.getDimensionValuesForDebug();
    if (dims1.size() != dims2.size()) {
      throw new SegmentValidationException("Dim lengths not equal %s vs %s", dims1, dims2);
    }
    final List<String> dim1Names = adapter1.getDimensionNames();
    final List<String> dim2Names = adapter2.getDimensionNames();
    int dimCount = dims1.size();
    for (int i = 0; i < dimCount; ++i) {
      final String dim1Name = dim1Names.get(i);
      final String dim2Name = dim2Names.get(i);

      ColumnCapabilities capabilities1 = adapter1.getCapabilities(dim1Name);
      ColumnCapabilities capabilities2 = adapter2.getCapabilities(dim2Name);
      ValueType dim1Type = capabilities1.getType();
      ValueType dim2Type = capabilities2.getType();
      if (dim1Type != dim2Type) {
        throw new SegmentValidationException(
            "Dim [%s] types not equal. Expected %d found %d",
            dim1Name,
            dim1Type,
            dim2Type
        );
      }

      Object vals1 = dims1.get(i);
      Object vals2 = dims2.get(i);
      if (isNullRow(vals1) ^ isNullRow(vals2)) {
        throw notEqualValidationException(dim1Name, vals1, vals2);
      }
      boolean vals1IsList = vals1 instanceof List;
      boolean vals2IsList = vals2 instanceof List;
      if (vals1IsList ^ vals2IsList) {
        if (vals1IsList) {
          if (((List) vals1).size() != 1 || !Objects.equals(((List) vals1).get(0), vals2)) {
            throw notEqualValidationException(dim1Name, vals1, vals2);
          }
        } else {
          if (((List) vals2).size() != 1 || !Objects.equals(((List) vals2).get(0), vals1)) {
            throw notEqualValidationException(dim1Name, vals1, vals2);
          }
        }
      } else {
        if (!Objects.equals(vals1, vals2)) {
          throw notEqualValidationException(dim1Name, vals1, vals2);
        }
      }
    }
  }

  private static boolean isNullRow(@Nullable Object row)
  {
    if (row == null) {
      return true;
    }
    if (!(row instanceof List)) {
      return false;
    }
    List<?> rowAsList = (List<?>) row;
    //noinspection ForLoopReplaceableByForEach -- in order to not create a garbage iterator object
    for (int i = 0, rowSize = rowAsList.size(); i < rowSize; i++) {
      Object v = rowAsList.get(i);
      //noinspection VariableNotUsedInsideIf
      if (v != null) {
        return false;
      }
    }
    return true;
  }

  private static SegmentValidationException notEqualValidationException(String dimName, Object v1, Object v2)
  {
    return new SegmentValidationException("Dim [%s] values not equal. Expected %s found %s", dimName, v1, v2);
  }

  public static class DefaultIndexIOHandler implements IndexIOHandler
  {
    private static final Logger log = new Logger(DefaultIndexIOHandler.class);

    @Override
    public MMappedIndex mapDir(File inDir) throws IOException
    {
      log.debug("Mapping v8 index[%s]", inDir);
      long startTime = System.currentTimeMillis();

      InputStream indexIn = null;
      try {
        indexIn = new FileInputStream(new File(inDir, "index.drd"));
        byte theVersion = (byte) indexIn.read();
        if (theVersion != V8_VERSION) {
          throw new IAE("Unknown version[%d]", theVersion);
        }
      }
      finally {
        Closeables.close(indexIn, false);
      }

      SmooshedFileMapper smooshedFiles = Smoosh.map(inDir);
      ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");

      indexBuffer.get(); // Skip the version byte
      final GenericIndexed<String> availableDimensions = GenericIndexed.read(
          indexBuffer,
          GenericIndexed.STRING_STRATEGY,
          smooshedFiles
      );
      final GenericIndexed<String> availableMetrics = GenericIndexed.read(
          indexBuffer,
          GenericIndexed.STRING_STRATEGY,
          smooshedFiles
      );
      final Interval dataInterval = Intervals.of(SERIALIZER_UTILS.readString(indexBuffer));
      final BitmapSerdeFactory bitmapSerdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();

      CompressedColumnarLongsSupplier timestamps = CompressedColumnarLongsSupplier.fromByteBuffer(
          smooshedFiles.mapFile(makeTimeFile(inDir, BYTE_ORDER).getName()),
          BYTE_ORDER
      );

      Map<String, MetricHolder> metrics = Maps.newLinkedHashMap();
      for (String metric : availableMetrics) {
        final String metricFilename = makeMetricFile(inDir, metric, BYTE_ORDER).getName();
        final MetricHolder holder = MetricHolder.fromByteBuffer(smooshedFiles.mapFile(metricFilename));

        if (!metric.equals(holder.getName())) {
          throw new ISE("Metric[%s] loaded up metric[%s] from disk.  File names do matter.", metric, holder.getName());
        }
        metrics.put(metric, holder);
      }

      Map<String, GenericIndexed<String>> dimValueLookups = new HashMap<>();
      Map<String, VSizeColumnarMultiInts> dimColumns = new HashMap<>();
      Map<String, GenericIndexed<ImmutableBitmap>> bitmaps = new HashMap<>();

      for (String dimension : IndexedIterable.create(availableDimensions)) {
        ByteBuffer dimBuffer = smooshedFiles.mapFile(makeDimFile(inDir, dimension).getName());
        String fileDimensionName = SERIALIZER_UTILS.readString(dimBuffer);
        Preconditions.checkState(
            dimension.equals(fileDimensionName),
            "Dimension file[%s] has dimension[%s] in it!?",
            makeDimFile(inDir, dimension),
            fileDimensionName
        );

        dimValueLookups.put(dimension, GenericIndexed.read(dimBuffer, GenericIndexed.STRING_STRATEGY));
        dimColumns.put(dimension, VSizeColumnarMultiInts.readFromByteBuffer(dimBuffer));
      }

      ByteBuffer invertedBuffer = smooshedFiles.mapFile("inverted.drd");
      for (int i = 0; i < availableDimensions.size(); ++i) {
        bitmaps.put(
            SERIALIZER_UTILS.readString(invertedBuffer),
            GenericIndexed.read(invertedBuffer, bitmapSerdeFactory.getObjectStrategy())
        );
      }

      Map<String, ImmutableRTree> spatialIndexed = new HashMap<>();
      ByteBuffer spatialBuffer = smooshedFiles.mapFile("spatial.drd");
      while (spatialBuffer != null && spatialBuffer.hasRemaining()) {
        spatialIndexed.put(
            SERIALIZER_UTILS.readString(spatialBuffer),
            new ImmutableRTreeObjectStrategy(bitmapSerdeFactory.getBitmapFactory()).fromByteBufferWithSize(
                spatialBuffer
            )
        );
      }

      final MMappedIndex retVal = new MMappedIndex(
          availableDimensions,
          availableMetrics,
          dataInterval,
          timestamps,
          metrics,
          dimValueLookups,
          dimColumns,
          bitmaps,
          spatialIndexed,
          smooshedFiles
      );

      log.debug("Mapped v8 index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);

      return retVal;
    }
  }

  interface IndexLoader
  {
    QueryableIndex load(File inDir, ObjectMapper mapper) throws IOException;
  }

  static class LegacyIndexLoader implements IndexLoader
  {
    private final IndexIOHandler legacyHandler;
    private final ColumnConfig columnConfig;

    LegacyIndexLoader(IndexIOHandler legacyHandler, ColumnConfig columnConfig)
    {
      this.legacyHandler = legacyHandler;
      this.columnConfig = columnConfig;
    }

    @Override
    public QueryableIndex load(File inDir, ObjectMapper mapper) throws IOException
    {
      MMappedIndex index = legacyHandler.mapDir(inDir);

      Map<String, ColumnHolder> columns = new HashMap<>();

      for (String dimension : index.getAvailableDimensions()) {
        ColumnBuilder builder = new ColumnBuilder()
            .setType(ValueType.STRING)
            .setHasMultipleValues(true)
            .setDictionaryEncodedColumnSupplier(
                new DictionaryEncodedColumnSupplier(
                    index.getDimValueLookup(dimension),
                    null,
                    Suppliers.ofInstance(index.getDimColumn(dimension)),
                    columnConfig.columnCacheSizeBytes()
                )
            )
            .setBitmapIndex(
                new BitmapIndexColumnPartSupplier(
                    new ConciseBitmapFactory(),
                    index.getBitmapIndexes().get(dimension),
                    index.getDimValueLookup(dimension)
                )
            );
        if (index.getSpatialIndexes().get(dimension) != null) {
          builder.setSpatialIndex(new SpatialIndexColumnPartSupplier(index.getSpatialIndexes().get(dimension)));
        }
        columns.put(
            dimension,
            builder.build()
        );
      }

      for (String metric : index.getAvailableMetrics()) {
        final MetricHolder metricHolder = index.getMetricHolder(metric);
        if (metricHolder.getType() == MetricHolder.MetricType.FLOAT) {
          columns.put(
              metric,
              new ColumnBuilder()
                  .setType(ValueType.FLOAT)
                  .setNumericColumnSupplier(
                      new FloatNumericColumnSupplier(
                          metricHolder.floatType,
                          LEGACY_FACTORY.getBitmapFactory().makeEmptyImmutableBitmap()
                      )
                  )
                  .build()
          );
        } else if (metricHolder.getType() == MetricHolder.MetricType.COMPLEX) {
          columns.put(
              metric,
              new ColumnBuilder()
                  .setType(ValueType.COMPLEX)
                  .setComplexColumnSupplier(
                      new ComplexColumnPartSupplier(metricHolder.getTypeName(), metricHolder.complexType)
                  )
                  .build()
          );
        }
      }

      columns.put(
          ColumnHolder.TIME_COLUMN_NAME,
          new ColumnBuilder()
              .setType(ValueType.LONG)
              .setNumericColumnSupplier(
                  new LongNumericColumnSupplier(
                      index.timestamps,
                      LEGACY_FACTORY.getBitmapFactory().makeEmptyImmutableBitmap()
                  )
              )
              .build()
      );
      return new SimpleQueryableIndex(
          index.getDataInterval(),
          index.getAvailableDimensions(),
          new ConciseBitmapFactory(),
          columns,
          index.getFileMapper(),
          null
      );
    }
  }

  static class V9IndexLoader implements IndexLoader
  {
    private final ColumnConfig columnConfig;

    V9IndexLoader(ColumnConfig columnConfig)
    {
      this.columnConfig = columnConfig;
    }

    @Override
    public QueryableIndex load(File inDir, ObjectMapper mapper) throws IOException
    {
      log.debug("Mapping v9 index[%s]", inDir);
      long startTime = System.currentTimeMillis();

      final int theVersion = Ints.fromByteArray(Files.toByteArray(new File(inDir, "version.bin")));
      if (theVersion != V9_VERSION) {
        throw new IAE("Expected version[9], got[%d]", theVersion);
      }

      SmooshedFileMapper smooshedFiles = Smoosh.map(inDir);

      ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");
      /**
       * Index.drd should consist of the segment version, the columns and dimensions of the segment as generic
       * indexes, the interval start and end millis as longs (in 16 bytes), and a bitmap index type.
       */
      final GenericIndexed<String> cols = GenericIndexed.read(
          indexBuffer,
          GenericIndexed.STRING_STRATEGY,
          smooshedFiles
      );
      final GenericIndexed<String> dims = GenericIndexed.read(
          indexBuffer,
          GenericIndexed.STRING_STRATEGY,
          smooshedFiles
      );
      final Interval dataInterval = Intervals.utc(indexBuffer.getLong(), indexBuffer.getLong());
      final BitmapSerdeFactory segmentBitmapSerdeFactory;

      /**
       * This is a workaround for the fact that in v8 segments, we have no information about the type of bitmap
       * index to use. Since we cannot very cleanly build v9 segments directly, we are using a workaround where
       * this information is appended to the end of index.drd.
       */
      if (indexBuffer.hasRemaining()) {
        segmentBitmapSerdeFactory = mapper.readValue(SERIALIZER_UTILS.readString(indexBuffer), BitmapSerdeFactory.class);
      } else {
        segmentBitmapSerdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();
      }

      Metadata metadata = null;
      ByteBuffer metadataBB = smooshedFiles.mapFile("metadata.drd");
      if (metadataBB != null) {
        try {
          metadata = mapper.readValue(
              SERIALIZER_UTILS.readBytes(metadataBB, metadataBB.remaining()),
              Metadata.class
          );
        }
        catch (JsonParseException | JsonMappingException ex) {
          // Any jackson deserialization errors are ignored e.g. if metadata contains some aggregator which
          // is no longer supported then it is OK to not use the metadata instead of failing segment loading
          log.warn(ex, "Failed to load metadata for segment [%s]", inDir);
        }
        catch (IOException ex) {
          throw new IOException("Failed to read metadata", ex);
        }
      }

      Map<String, ColumnHolder> columns = new HashMap<>();

      for (String columnName : cols) {
        if (Strings.isNullOrEmpty(columnName)) {
          log.warn("Null or Empty Dimension found in the file : " + inDir);
          continue;
        }
        columns.put(columnName, deserializeColumn(mapper, smooshedFiles.mapFile(columnName), smooshedFiles));
      }

      columns.put(ColumnHolder.TIME_COLUMN_NAME, deserializeColumn(mapper, smooshedFiles.mapFile("__time"), smooshedFiles));

      final QueryableIndex index = new SimpleQueryableIndex(
          dataInterval,
          dims,
          segmentBitmapSerdeFactory.getBitmapFactory(),
          columns,
          smooshedFiles,
          metadata
      );

      log.debug("Mapped v9 index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);

      return index;
    }

    private ColumnHolder deserializeColumn(ObjectMapper mapper, ByteBuffer byteBuffer, SmooshedFileMapper smooshedFiles)
        throws IOException
    {
      ColumnDescriptor serde = mapper.readValue(
          SERIALIZER_UTILS.readString(byteBuffer), ColumnDescriptor.class
      );
      return serde.read(byteBuffer, columnConfig, smooshedFiles);
    }
  }

  public static File makeDimFile(File dir, String dimension)
  {
    return new File(dir, StringUtils.format("dim_%s.drd", dimension));
  }

  public static File makeTimeFile(File dir, ByteOrder order)
  {
    return new File(dir, StringUtils.format("time_%s.drd", order));
  }

  public static File makeMetricFile(File dir, String metricName, ByteOrder order)
  {
    return new File(dir, StringUtils.format("met_%s_%s.drd", metricName, order));
  }
}
