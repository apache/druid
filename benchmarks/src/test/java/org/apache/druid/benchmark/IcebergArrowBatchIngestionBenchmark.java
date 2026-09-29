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

package org.apache.druid.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.BatchToInputRowIterator;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.iceberg.input.IcebergArrowInputSourceReader;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(1)
public class IcebergArrowBatchIngestionBenchmark
{
  private static final String NAMESPACE = "bench";
  private static final String TABLE_NAME = "batch_ingestion";
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final IndexMergerV9 INDEX_MERGER = new IndexMergerV9(
      JSON_MAPPER,
      new IndexIO(JSON_MAPPER, ColumnConfig.DEFAULT),
      OffHeapMemorySegmentWriteOutMediumFactory.instance()
  );

  public enum ReadPath
  {
    LEGACY_MAP,
    BATCH_BACKED
  }

  @Param({"LEGACY_MAP", "BATCH_BACKED"})
  public ReadPath readPath;

  @Param({"100000"})
  public int numRows;

  @Param({"5"})
  public int numColumns;

  @Param({"1024"})
  public int batchSize;

  private File warehouseDirectory;
  private File persistDirectory;
  private Catalog catalog;
  private Table table;
  private InputRowSchema inputRowSchema;
  private AggregatorFactory[] metrics;
  private List<String> dataColumnNames;
  private long expectedChecksum;

  @Setup(Level.Trial)
  public void setupTrial() throws Exception
  {
    warehouseDirectory = FileUtils.createTempDir();
    final Configuration configuration = new Configuration();
    configuration.set("fs.file.impl", LocalFileSystem.class.getName());
    final HadoopCatalog hadoopCatalog = new HadoopCatalog();
    hadoopCatalog.setConf(configuration);
    hadoopCatalog.initialize("hadoop", Map.of("warehouse", warehouseDirectory.getAbsolutePath()));
    catalog = hadoopCatalog;
    final Schema icebergSchema = buildIcebergSchema();
    inputRowSchema = buildInputRowSchema();
    metrics = buildMetrics();
    dataColumnNames = icebergSchema.columns()
                                   .stream()
                                   .map(Types.NestedField::name)
                                   .filter(name -> !"ts".equals(name))
                                   .toList();

    table = catalog.createTable(TableIdentifier.of(Namespace.of(NAMESPACE), TABLE_NAME), icebergSchema);
    writeData(icebergSchema);

    expectedChecksum = readChecksum(ReadPath.LEGACY_MAP);
    final long batchChecksum = readChecksum(ReadPath.BATCH_BACKED);
    if (expectedChecksum != batchChecksum) {
      throw new IllegalStateException("Reader checksums do not match");
    }
  }

  @TearDown(Level.Trial)
  public void teardownTrial() throws IOException
  {
    if (catalog != null) {
      catalog.dropTable(TableIdentifier.of(Namespace.of(NAMESPACE), TABLE_NAME));
    }
    if (warehouseDirectory != null) {
      FileUtils.deleteDirectory(warehouseDirectory);
    }
  }

  @Setup(Level.Invocation)
  public void setupInvocation()
  {
    persistDirectory = FileUtils.createTempDir();
  }

  @TearDown(Level.Invocation)
  public void teardownInvocation() throws IOException
  {
    FileUtils.deleteDirectory(persistDirectory);
  }

  @Benchmark
  public long readRequiredColumns() throws Exception
  {
    final long checksum = readChecksum(readPath);
    if (checksum != expectedChecksum) {
      throw new IllegalStateException("Reader checksum changed");
    }
    return checksum;
  }

  @Benchmark
  public int readAndIndex() throws Exception
  {
    try (IncrementalIndex index = readIntoIndex()) {
      return index.numRows();
    }
  }

  @Benchmark
  public int readIndexAndPersist() throws Exception
  {
    try (IncrementalIndex index = readIntoIndex()) {
      final File segmentDirectory = INDEX_MERGER.persist(
          index,
          persistDirectory,
          IndexSpec.getDefault(),
          null
      );
      if (!segmentDirectory.isDirectory()) {
        throw new IllegalStateException("Segment was not persisted");
      }
      return index.numRows();
    }
  }

  private long readChecksum(final ReadPath path) throws Exception
  {
    long checksum = 1;
    int rowsRead = 0;
    try (CloseableIterator<InputRow> rows = openRows(path)) {
      while (rows.hasNext()) {
        final InputRow row = rows.next();
        checksum = 31 * checksum + row.getTimestampFromEpoch();
        for (final String columnName : dataColumnNames) {
          checksum = 31 * checksum + Objects.hashCode(row.getRaw(columnName));
        }
        rowsRead++;
      }
    }
    verifyRowCount(rowsRead);
    return checksum;
  }

  private IncrementalIndex readIntoIndex() throws Exception
  {
    final IncrementalIndex index = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMetrics(metrics)
                .withRollup(false)
                .build()
        )
        .setMaxRowCount(numRows + 1)
        .build();
    int rowsRead = 0;
    try (CloseableIterator<InputRow> rows = openRows(readPath)) {
      while (rows.hasNext()) {
        index.add(rows.next());
        rowsRead++;
      }
    }
    catch (Exception e) {
      index.close();
      throw e;
    }
    verifyRowCount(rowsRead);
    return index;
  }

  private CloseableIterator<InputRow> openRows(final ReadPath path) throws IOException
  {
    final IcebergArrowInputSourceReader reader = new IcebergArrowInputSourceReader(
        table,
        null,
        null,
        true,
        inputRowSchema,
        batchSize
    );
    if (path == ReadPath.BATCH_BACKED) {
      return new BatchToInputRowIterator(reader.readBatches(null), inputRowSchema);
    }
    return reader.read();
  }

  private void verifyRowCount(final int rowsRead)
  {
    if (rowsRead != numRows) {
      throw new IllegalStateException("Expected " + numRows + " rows but read " + rowsRead);
    }
  }

  private Schema buildIcebergSchema()
  {
    final List<Types.NestedField> fields = new ArrayList<>();
    fields.add(Types.NestedField.required(1, "ts", Types.LongType.get()));
    for (int i = 2; i <= numColumns; i++) {
      if (i % 3 == 0) {
        fields.add(Types.NestedField.optional(i, "double_" + i, Types.DoubleType.get()));
      } else if (i % 3 == 1) {
        fields.add(Types.NestedField.optional(i, "long_" + i, Types.LongType.get()));
      } else {
        fields.add(Types.NestedField.optional(i, "string_" + i, Types.StringType.get()));
      }
    }
    return new Schema(fields);
  }

  private InputRowSchema buildInputRowSchema()
  {
    final List<DimensionSchema> dimensions = new ArrayList<>();
    for (int i = 2; i <= numColumns; i++) {
      if (i % 3 == 0) {
        dimensions.add(new DoubleDimensionSchema("double_" + i));
      } else if (i % 3 == 1) {
        dimensions.add(new LongDimensionSchema("long_" + i));
      } else {
        dimensions.add(new StringDimensionSchema("string_" + i));
      }
    }
    return new InputRowSchema(
        new TimestampSpec("ts", "millis", null),
        DimensionsSpec.builder().setDimensions(dimensions).build(),
        ColumnsFilter.all()
    );
  }

  private AggregatorFactory[] buildMetrics()
  {
    final List<AggregatorFactory> aggregators = new ArrayList<>();
    aggregators.add(new CountAggregatorFactory("count"));
    for (int i = 2; i <= numColumns; i++) {
      if (i % 3 == 0) {
        aggregators.add(new DoubleSumAggregatorFactory("sum", "double_" + i));
        break;
      }
    }
    return aggregators.toArray(new AggregatorFactory[0]);
  }

  private void writeData(final Schema icebergSchema) throws IOException
  {
    final String filePath = table.location() + "/" + UUID.randomUUID() + ".parquet";
    final OutputFile outputFile = table.io().newOutputFile(filePath);
    final DataWriter<GenericRecord> writer = Parquet.writeData(outputFile)
                                                    .schema(icebergSchema)
                                                    .createWriterFunc(GenericParquetWriter::create)
                                                    .overwrite()
                                                    .withSpec(PartitionSpec.unpartitioned())
                                                    .build();
    try {
      final GenericRecord template = GenericRecord.create(icebergSchema);
      for (int rowNumber = 0; rowNumber < numRows; rowNumber++) {
        final GenericRecord record = template.copy();
        record.setField("ts", (rowNumber + 1) * 1000L);
        for (final Types.NestedField field : icebergSchema.columns()) {
          if (field.name().startsWith("double_")) {
            record.setField(field.name(), rowNumber * 0.1D);
          } else if (field.name().startsWith("long_")) {
            record.setField(field.name(), (long) rowNumber);
          } else if (field.name().startsWith("string_")) {
            record.setField(field.name(), "value_" + rowNumber % 1000);
          }
        }
        writer.write(record);
      }
    }
    finally {
      writer.close();
    }
    final DataFile dataFile = writer.toDataFile();
    table.newAppend().appendFile(dataFile).commit();
  }
}
