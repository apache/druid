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

package org.apache.druid.cli;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.netty.util.SuppressForbidden;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.metadata.metadata.ListColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.nested.CompressedNestedDataComplexColumn;
import org.apache.druid.segment.nested.NestedFieldLiteralDictionaryEncodedColumn;
import org.apache.druid.segment.nested.NestedPathFinder;
import org.apache.druid.segment.nested.NestedPathPart;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.roaringbitmap.IntIterator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Command(
    name = "dump-segment",
    description = "Dump segment data"
)
public class DumpSegment extends GuiceRunnable
{
  private static final Logger log = new Logger(DumpSegment.class);

  private enum DumpType
  {
    ROWS,
    METADATA,
    BITMAPS,
    NESTED
  }

  public DumpSegment()
  {
    super(log);
  }

  @Option(
      name = {"-d", "--directory"},
      title = "directory",
      description = "Directory containing segment data.")
  @Required
  public String directory;

  @Option(
      name = {"-o", "--out"},
      title = "file",
      description = "File to write to, or omit to write to stdout.")
  public String outputFileName;

  @Option(
      name = {"--filter"},
      title = "json",
      description = "Filter, JSON encoded, or omit to include all rows. Only used if dumping rows.")
  public String filterJson = null;

  @Option(
      name = {"-c", "--column"},
      title = "column",
      description = "Column to include, specify multiple times for multiple columns, or omit to include all columns.")
  public List<String> columnNamesFromCli = new ArrayList<>();

  @Option(
      name = {"--nested-path"},
      title = "nested path",
      description = "JSONPath expression for nested column")
  public String nestedPath;

  @Option(
      name = "--time-iso8601",
      title = "Format __time column in ISO8601 format rather than long. Only used if dumping rows.")
  public boolean timeISO8601 = false;

  @Option(
      name = "--dump",
      title = "type",
      description = "Dump either 'rows' (default), 'metadata', or 'bitmaps'")
  public String dumpTypeString = DumpType.ROWS.toString();

  @Option(
      name = "--decompress-bitmaps",
      title = "Dump bitmaps as arrays rather than base64-encoded compressed bitmaps. Only used if dumping bitmaps.")
  public boolean decompressBitmaps = false;

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    final IndexIO indexIO = injector.getInstance(IndexIO.class);
    final DumpType dumpType;

    try {
      dumpType = DumpType.valueOf(StringUtils.toUpperCase(dumpTypeString));
    }
    catch (Exception e) {
      throw new IAE("Not a valid dump type: %s", dumpTypeString);
    }

    try (final QueryableIndex index = indexIO.loadIndex(new File(directory))) {
      switch (dumpType) {
        case ROWS:
          runDump(injector, index);
          break;
        case METADATA:
          runMetadata(injector, index);
          break;
        case BITMAPS:
          runBitmaps(injector, outputFileName, index, getColumnsToInclude(index), decompressBitmaps);
          break;
        case NESTED:
          Preconditions.checkArgument(
              columnNamesFromCli.size() == 1,
              "Must be exactly 1 column specified"
          );
          final String nestedColumn = columnNamesFromCli.get(0);
          if (nestedPath == null) {
            runDumpNestedColumn(injector, outputFileName, index, nestedColumn);
          } else {
            runDumpNestedColumnPath(injector, outputFileName, index, nestedColumn, nestedPath);
          }
          break;
        default:
          throw new ISE("dumpType[%s] has no handler", dumpType);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void runMetadata(final Injector injector, final QueryableIndex index) throws IOException
  {
    final ObjectMapper objectMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class))
                                              .copy()
                                              .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    final SegmentMetadataQuery query = new SegmentMetadataQuery(
        new TableDataSource("dataSource"),
        new SpecificSegmentSpec(new SegmentDescriptor(index.getDataInterval(), "0", 0)),
        new ListColumnIncluderator(getColumnsToInclude(index)),
        false,
        null,
        EnumSet.allOf(SegmentMetadataQuery.AnalysisType.class),
        false,
        false
    );
    withOutputStream(
        new Function<OutputStream, Object>()
        {
          @Override
          public Object apply(final OutputStream out)
          {
            evaluateSequenceForSideEffects(
                Sequences.map(
                    executeQuery(injector, index, query),
                    new Function<SegmentAnalysis, Object>()
                    {
                      @Override
                      public Object apply(SegmentAnalysis analysis)
                      {
                        try {
                          objectMapper.writeValue(out, analysis);
                        }
                        catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                        return null;
                      }
                    }
                )
            );

            return null;
          }
        }
    );
  }

  private void runDump(final Injector injector, final QueryableIndex index) throws IOException
  {
    final ObjectMapper objectMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);
    final List<String> columnNames = getColumnsToInclude(index);
    final DimFilter filter = filterJson != null ? objectMapper.readValue(filterJson, DimFilter.class) : null;

    final Sequence<Cursor> cursors = adapter.makeCursors(
        Filters.toFilter(filter),
        index.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    withOutputStream(
        new Function<OutputStream, Object>()
        {
          @Override
          public Object apply(final OutputStream out)
          {
            final Sequence<Object> sequence = Sequences.map(
                cursors,
                new Function<Cursor, Object>()
                {
                  @Override
                  public Object apply(Cursor cursor)
                  {
                    ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
                    final List<BaseObjectColumnValueSelector> selectors = columnNames
                        .stream()
                        .map(columnSelectorFactory::makeColumnValueSelector)
                        .collect(Collectors.toList());

                    while (!cursor.isDone()) {
                      final Map<String, Object> row = Maps.newLinkedHashMap();

                      for (int i = 0; i < columnNames.size(); i++) {
                        final String columnName = columnNames.get(i);
                        final Object value = selectors.get(i).getObject();

                        if (timeISO8601 && columnNames.get(i).equals(ColumnHolder.TIME_COLUMN_NAME)) {
                          row.put(columnName, new DateTime(value, DateTimeZone.UTC).toString());
                        } else {
                          row.put(columnName, value);
                        }
                      }

                      try {
                        out.write(objectMapper.writeValueAsBytes(row));
                        out.write('\n');
                      }
                      catch (IOException e) {
                        throw new RuntimeException(e);
                      }

                      cursor.advance();
                    }

                    return null;
                  }
                }
            );

            evaluateSequenceForSideEffects(sequence);

            return null;
          }
        }
    );
  }

  @VisibleForTesting
  public static void runBitmaps(
      final Injector injector,
      final String outputFileName,
      final QueryableIndex index,
      List<String> columnNames,
      boolean decompressBitmaps
  ) throws IOException
  {
    final ObjectMapper objectMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();
    final BitmapSerdeFactory bitmapSerdeFactory;

    if (bitmapFactory instanceof ConciseBitmapFactory) {
      bitmapSerdeFactory = new ConciseBitmapSerdeFactory();
    } else if (bitmapFactory instanceof RoaringBitmapFactory) {
      bitmapSerdeFactory = new RoaringBitmapSerdeFactory(null);
    } else {
      throw new ISE(
          "Don't know which BitmapSerdeFactory to use for BitmapFactory[%s]!",
          bitmapFactory.getClass().getName()
      );
    }

    withOutputStream(
        out -> {
          try (final JsonGenerator jg = objectMapper.getFactory().createGenerator(out)) {
            jg.writeStartObject();
            {
              jg.writeObjectField("bitmapSerdeFactory", bitmapSerdeFactory);
              jg.writeFieldName("bitmaps");
              jg.writeStartObject();
              {
                for (final String columnName : columnNames) {
                  final ColumnHolder columnHolder = index.getColumnHolder(columnName);
                  final ColumnIndexSupplier indexSupplier = columnHolder.getIndexSupplier();
                  if (indexSupplier == null) {
                    jg.writeNullField(columnName);
                  } else {
                    DictionaryEncodedStringValueIndex valueIndex =
                        indexSupplier.as(DictionaryEncodedStringValueIndex.class);
                    if (valueIndex == null) {
                      jg.writeNullField(columnName);
                    } else {
                      jg.writeFieldName(columnName);
                      jg.writeStartObject();
                      for (int i = 0; i < valueIndex.getCardinality(); i++) {
                        String val = valueIndex.getValue(i);
                        // respect nulls if they are present in the dictionary
                        jg.writeFieldName(val == null ? "null" : val);
                        final ImmutableBitmap bitmap = valueIndex.getBitmap(i);
                        if (decompressBitmaps) {
                          jg.writeStartArray();
                          final IntIterator iterator = bitmap.iterator();
                          while (iterator.hasNext()) {
                            final int rowNum = iterator.next();
                            jg.writeNumber(rowNum);
                          }
                          jg.writeEndArray();
                        } else {
                          byte[] bytes = bitmapSerdeFactory.getObjectStrategy().toBytes(bitmap);
                          if (bytes != null) {
                            jg.writeBinary(bytes);
                          }
                        }
                      }
                      jg.writeEndObject();
                    }
                  }
                }
              }
              jg.writeEndObject();
            }
            jg.writeEndObject();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }

          return null;
        },
        outputFileName
    );
  }

  @VisibleForTesting
  public static void runDumpNestedColumn(
      final Injector injector,
      final String outputFileName,
      final QueryableIndex index,
      String columnName
  ) throws IOException
  {
    final ObjectMapper objectMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));

    withOutputStream(
        out -> {
          try (final JsonGenerator jg = objectMapper.getFactory().createGenerator(out)) {
            jg.writeStartObject();
            {
              jg.writeFieldName(columnName);
              jg.writeStartObject();
              {
                final ColumnHolder columnHolder = index.getColumnHolder(columnName);
                final BaseColumn baseColumn = columnHolder.getColumn();
                Preconditions.checkArgument(baseColumn instanceof CompressedNestedDataComplexColumn);
                final CompressedNestedDataComplexColumn<?> nestedDataColumn =
                    (CompressedNestedDataComplexColumn<?>) baseColumn;

                jg.writeFieldName("fields");
                jg.writeStartArray();
                List<List<NestedPathPart>> fields = nestedDataColumn.getNestedFields();
                for (List<NestedPathPart> field : fields) {
                  jg.writeStartObject();
                  jg.writeFieldName("path");
                  jg.writeString(NestedPathFinder.toNormalizedJsonPath(field));
                  jg.writeFieldName("types");
                  Set<ColumnType> types = nestedDataColumn.getColumnTypes(field);
                  jg.writeStartArray();
                  for (ColumnType type : types) {
                    jg.writeString(type.asTypeString());
                  }
                  jg.writeEndArray();
                  jg.writeEndObject();
                }
                jg.writeEndArray();

                Indexed<ByteBuffer> globalStringDictionary = nestedDataColumn.getStringDictionary();
                FixedIndexed<Long> globalLongDictionary = nestedDataColumn.getLongDictionary();
                FixedIndexed<Double> globalDoubleDictionary = nestedDataColumn.getDoubleDictionary();
                jg.writeFieldName("dictionaries");
                jg.writeStartObject();
                {
                  int globalId = 0;
                  jg.writeFieldName("strings");
                  jg.writeStartArray();
                  for (int i = 0; i < globalStringDictionary.size(); i++, globalId++) {
                    jg.writeStartObject();
                    jg.writeFieldName("globalId");
                    jg.writeNumber(globalId);
                    jg.writeFieldName("value");
                    final ByteBuffer val = globalStringDictionary.get(i);
                    if (val == null) {
                      jg.writeNull();
                    } else {
                      jg.writeString(StringUtils.fromUtf8(val));
                    }
                    jg.writeEndObject();
                  }
                  jg.writeEndArray();

                  jg.writeFieldName("longs");
                  jg.writeStartArray();
                  for (int i = 0; i < globalLongDictionary.size(); i++, globalId++) {
                    jg.writeStartObject();
                    jg.writeFieldName("globalId");
                    jg.writeNumber(globalId);
                    jg.writeFieldName("value");
                    jg.writeNumber(globalLongDictionary.get(i));
                    jg.writeEndObject();
                  }
                  jg.writeEndArray();

                  jg.writeFieldName("doubles");
                  jg.writeStartArray();
                  for (int i = 0; i < globalDoubleDictionary.size(); i++, globalId++) {
                    jg.writeStartObject();
                    jg.writeFieldName("globalId");
                    jg.writeNumber(globalId);
                    jg.writeFieldName("value");
                    jg.writeNumber(globalDoubleDictionary.get(i));
                    jg.writeEndObject();
                  }
                  jg.writeEndArray();
                }
                jg.writeFieldName("nullRows");
                ImmutableBitmap bitmap = nestedDataColumn.getNullValues();
                jg.writeStartArray();
                final IntIterator iterator = bitmap.iterator();
                while (iterator.hasNext()) {
                  final int rowNum = iterator.next();
                  jg.writeNumber(rowNum);
                }
                jg.writeEndArray();
                jg.writeEndObject();
              }
              jg.writeEndObject();
            }
            jg.writeEndObject();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }

          return null;
        },
        outputFileName
    );
  }

  @VisibleForTesting
  public static void runDumpNestedColumnPath(
      final Injector injector,
      final String outputFileName,
      final QueryableIndex index,
      String columnName,
      String path
  ) throws IOException
  {
    final ObjectMapper objectMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final SerializerProvider serializers = objectMapper.getSerializerProviderInstance();
    final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();
    final BitmapSerdeFactory bitmapSerdeFactory;

    if (bitmapFactory instanceof ConciseBitmapFactory) {
      bitmapSerdeFactory = new ConciseBitmapSerdeFactory();
    } else if (bitmapFactory instanceof RoaringBitmapFactory) {
      bitmapSerdeFactory = new RoaringBitmapSerdeFactory(null);
    } else {
      throw new ISE(
          "Don't know which BitmapSerdeFactory to use for BitmapFactory[%s]!",
          bitmapFactory.getClass().getName()
      );
    }

    withOutputStream(
        out -> {
          try (final JsonGenerator jg = objectMapper.getFactory().createGenerator(out)) {
            jg.writeStartObject();
            {
              jg.writeObjectField("bitmapSerdeFactory", bitmapSerdeFactory);
              jg.writeFieldName(columnName);
              jg.writeStartObject();
              {
                final ColumnHolder columnHolder = index.getColumnHolder(columnName);
                final BaseColumn column = columnHolder.getColumn();
                Preconditions.checkArgument(column instanceof CompressedNestedDataComplexColumn);
                final CompressedNestedDataComplexColumn nestedDataColumn = (CompressedNestedDataComplexColumn) column;
                final List<NestedPathPart> pathParts = NestedPathFinder.parseJsonPath(path);
                final ColumnIndexSupplier indexSupplier = nestedDataColumn.getColumnIndexSupplier(pathParts);

                final ColumnHolder nestedPathColumnHolder = nestedDataColumn.getColumnHolder(pathParts);
                final NestedFieldLiteralDictionaryEncodedColumn<?> nestedPathColumn =
                    (NestedFieldLiteralDictionaryEncodedColumn<?>) nestedPathColumnHolder.getColumn();
                final FixedIndexed<Integer> nestedPathDictionary = nestedPathColumn.getDictionary();

                SimpleAscendingOffset offset = new SimpleAscendingOffset(index.getNumRows());
                final ColumnValueSelector rawSelector = nestedDataColumn.makeColumnValueSelector(offset);
                final DimensionSelector fieldSelector = nestedDataColumn.makeDimensionSelector(
                    pathParts,
                    offset,
                    null
                );
                if (indexSupplier == null) {
                  jg.writeNullField(path);
                } else {
                  DictionaryEncodedStringValueIndex valueIndex =
                      indexSupplier.as(DictionaryEncodedStringValueIndex.class);
                  if (valueIndex == null) {
                    jg.writeNullField(path);
                  } else {
                    jg.writeFieldName(path);
                    jg.writeStartObject();
                    jg.writeFieldName("types");
                    Set<ColumnType> types = nestedDataColumn.getColumnTypes(pathParts);
                    jg.writeStartArray();
                    for (ColumnType type : types) {
                      jg.writeString(type.asTypeString());
                    }
                    jg.writeEndArray();

                    // write "dictionary":[{"localId":0, "value": "hello", "rows":[]}...]
                    jg.writeFieldName("dictionary");
                    jg.writeStartArray();
                    for (int i = 0; i < valueIndex.getCardinality(); i++) {
                      jg.writeStartObject();
                      jg.writeFieldName("localId");
                      jg.writeNumber(i);
                      jg.writeFieldName("globalId");
                      jg.writeNumber(nestedPathDictionary.get(i));
                      jg.writeFieldName("value");
                      String val = valueIndex.getValue(i);
                      if (val == null) {
                        jg.writeNull();
                      } else {
                        jg.writeString(val);
                      }
                      jg.writeFieldName("rows");
                      final ImmutableBitmap bitmap = valueIndex.getBitmap(i);
                      jg.writeStartArray();
                      final IntIterator iterator = bitmap.iterator();
                      while (iterator.hasNext()) {
                        final int rowNum = iterator.next();
                        jg.writeNumber(rowNum);
                      }
                      jg.writeEndArray();
                      jg.writeEndObject();
                    }
                    jg.writeEndArray();
                    jg.writeFieldName("column");
                    jg.writeStartArray();
                    for (int i = 0; i < index.getNumRows(); i++) {
                      jg.writeStartObject();
                      jg.writeFieldName("row");
                      jg.writeNumber(i);
                      jg.writeFieldName("raw");
                      JacksonUtils.writeObjectUsingSerializerProvider(jg, serializers, rawSelector.getObject());
                      jg.writeFieldName("fieldId");
                      int id = fieldSelector.getRow().get(0);
                      jg.writeNumber(id);
                      jg.writeFieldName("fieldValue");
                      String val = fieldSelector.lookupName(id);
                      if (val == null) {
                        jg.writeNull();
                      } else {
                        jg.writeString(val);
                      }
                      jg.writeEndObject();
                      offset.increment();
                    }
                    jg.writeEndArray();
                    jg.writeEndObject();
                  }
                }
                column.close();
              }
              jg.writeEndObject();
            }
            jg.writeEndObject();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }

          return null;
        },
        outputFileName
    );
  }

  private List<String> getColumnsToInclude(final QueryableIndex index)
  {
    final Set<String> columnNames = Sets.newLinkedHashSet(columnNamesFromCli);

    // Empty columnNames => include all columns.
    if (columnNames.isEmpty()) {
      columnNames.add(ColumnHolder.TIME_COLUMN_NAME);
      Iterables.addAll(columnNames, index.getColumnNames());
    } else {
      // Remove any provided columns that do not exist in this segment.
      for (String columnName : ImmutableList.copyOf(columnNames)) {
        if (index.getColumnHolder(columnName) == null) {
          columnNames.remove(columnName);
        }
      }
    }

    return ImmutableList.copyOf(columnNames);
  }

  private <T> T withOutputStream(Function<OutputStream, T> f) throws IOException
  {
    return withOutputStream(f, outputFileName);
  }

  @SuppressForbidden(reason = "System#out")
  private static <T> T withOutputStream(Function<OutputStream, T> f, String outputFileName) throws IOException
  {
    if (outputFileName == null) {
      return f.apply(System.out);
    } else {
      try (final OutputStream out = new FileOutputStream(outputFileName)) {
        return f.apply(out);
      }
    }
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new DruidProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/tool");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(9999);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
            binder.bind(DruidProcessingConfig.class).toInstance(
                new DruidProcessingConfig()
                {
                  @Override
                  public String getFormatString()
                  {
                    return "processing-%s";
                  }

                  @Override
                  public int intermediateComputeSizeBytes()
                  {
                    return 100 * 1024 * 1024;
                  }

                  @Override
                  public int getNumThreads()
                  {
                    return 1;
                  }

                  @Override
                  public int columnCacheSizeBytes()
                  {
                    return 25 * 1024 * 1024;
                  }
                }
            );
            binder.bind(ColumnConfig.class).to(DruidProcessingConfig.class);
          }
        }
    );
  }

  @VisibleForTesting
  static <T> Sequence<T> executeQuery(final Injector injector, final QueryableIndex index, final Query<T> query)
  {
    final QueryRunnerFactoryConglomerate conglomerate = injector.getInstance(QueryRunnerFactoryConglomerate.class);
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    final QueryRunner<T> runner = factory.createRunner(new QueryableIndexSegment(index, SegmentId.dummy("segment")));
    return factory
        .getToolchest()
        .mergeResults(factory.mergeRunners(DirectQueryProcessingPool.INSTANCE, ImmutableList.of(runner)))
        .run(QueryPlus.wrap(query), ResponseContext.createEmpty());
  }

  private static <T> void evaluateSequenceForSideEffects(final Sequence<T> sequence)
  {
    sequence.accumulate(null, (accumulated, in) -> null);
  }
}
