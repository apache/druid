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

package org.apache.druid.iceberg.input;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.LocalInputSourceFactory;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.iceberg.filter.IcebergEqualsFilter;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IcebergInputSourceTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private IcebergCatalog testCatalog;
  private TableIdentifier tableIdentifier;
  private File warehouseDir;

  private Schema tableSchema = new Schema(
      Types.NestedField.required(1, "id", Types.StringType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get())
  );
  private Map<String, Object> tableData = ImmutableMap.of("id", "123988", "name", "Foo");

  private static final String NAMESPACE = "default";
  private static final String TABLENAME = "foosTable";

  @Before
  public void setup() throws IOException
  {
    warehouseDir = FileUtils.createTempDir();
    testCatalog = new LocalCatalog(warehouseDir.getPath(), new HashMap<>(), true);
    tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), TABLENAME);

    createAndLoadTable(tableIdentifier);
  }

  @Test
  public void testInputSource() throws IOException
  {
    IcebergInputSource inputSource = new IcebergInputSource(
        TABLENAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );
    Stream<InputSplit<List<String>>> splits = inputSource.createSplits(null, new MaxSizeSplitHintSpec(null, null));
    List<File> localInputSourceList = splits.map(inputSource::withSplit)
                                            .map(inpSource -> (LocalInputSource) inpSource)
                                            .map(LocalInputSource::getFiles)
                                            .flatMap(List::stream)
                                            .collect(Collectors.toList());

    Assert.assertEquals(1, inputSource.estimateNumSplits(null, new MaxSizeSplitHintSpec(1L, null)));
    Assert.assertEquals(1, localInputSourceList.size());
    CloseableIterable<Record> datafileReader = Parquet.read(Files.localInput(localInputSourceList.get(0)))
                                                      .project(tableSchema)
                                                      .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(
                                                          tableSchema,
                                                          fileSchema
                                                      ))
                                                      .build();


    for (Record record : datafileReader) {
      Assert.assertEquals(tableData.get("id"), record.get(0));
      Assert.assertEquals(tableData.get("name"), record.get(1));
    }
  }

  // Reads the table without InputRowSchema and then once per column using InputRowSchema column filters.
  @Test
  public void testReadTableWithAndWithoutInputRowSchema() throws IOException
  {
    // Read the table with no InputRowSchema to preserve the legacy full-table scan behavior.
    final IcebergInputSource inputSourceWithoutSchema = new IcebergInputSource(
        TABLENAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    inputSourceWithoutSchema.retrieveIcebergDatafiles(null);

    final List<File> fullScanFiles = ((LocalInputSource) inputSourceWithoutSchema.getDelegateInputSource())
        .getFiles();
    Assert.assertEquals(1, fullScanFiles.size());

    try (final CloseableIterable<Record> datafileReader = Parquet.read(Files.localInput(fullScanFiles.get(0)))
                                                                 .project(tableSchema)
                                                                 .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(
                                                                     tableSchema,
                                                                     fileSchema
                                                                 ))
                                                                 .build()) {
      for (Record record : datafileReader) {
        Assert.assertEquals(tableData.get("id"), record.get(0));
        Assert.assertEquals(tableData.get("name"), record.get(1));
      }
    }

    // Read the table once per column using InputRowSchema column filters to verify single-column projections.
    for (final Types.NestedField column : tableSchema.columns()) {
      final IcebergInputSource inputSourceWithSchema = new IcebergInputSource(
          TABLENAME,
          NAMESPACE,
          null,
          testCatalog,
          new LocalInputSourceFactory(),
          null,
          null
      );
      final InputRowSchema inputRowSchema = createInputRowSchema(
          ColumnsFilter.inclusionBased(ImmutableSet.of(column.name()))
      );

      inputSourceWithSchema.retrieveIcebergDatafiles(inputRowSchema);

      final List<File> projectedFiles = ((LocalInputSource) inputSourceWithSchema.getDelegateInputSource())
          .getFiles();
      Assert.assertEquals(1, projectedFiles.size());

      final Schema projectedSchema = new Schema(column);
      try (final CloseableIterable<Record> datafileReader = Parquet.read(Files.localInput(projectedFiles.get(0)))
                                                                   .project(projectedSchema)
                                                                   .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(
                                                                       projectedSchema,
                                                                       fileSchema
                                                                   ))
                                                                   .build()) {
        for (Record record : datafileReader) {
          Assert.assertEquals(1, record.size());
          Assert.assertEquals(tableData.get(column.name()), record.get(0));
        }
      }
    }
  }

  private InputRowSchema createInputRowSchema(final ColumnsFilter columnsFilter)
  {
    return new InputRowSchema(
        new TimestampSpec("id", "auto", null),
        new DimensionsSpec(
            ImmutableList.of(
                new StringDimensionSchema("id"),
                new StringDimensionSchema("name")
            )
        ),
        columnsFilter
    );
  }

  @Test
  public void testInputSourceWithEmptySource() throws IOException
  {
    IcebergInputSource inputSource = new IcebergInputSource(
        TABLENAME,
        NAMESPACE,
        new IcebergEqualsFilter("id", "0000"),
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );
    Stream<InputSplit<List<String>>> splits = inputSource.createSplits(null, new MaxSizeSplitHintSpec(null, null));
    Assert.assertEquals(0, splits.count());
  }

  @Test
  public void testInputSourceWithFilter() throws IOException
  {
    IcebergInputSource inputSource = new IcebergInputSource(
        TABLENAME,
        NAMESPACE,
        new IcebergEqualsFilter("id", "123988"),
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );
    Stream<InputSplit<List<String>>> splits = inputSource.createSplits(null, new MaxSizeSplitHintSpec(null, null));
    List<File> localInputSourceList = splits.map(inputSource::withSplit)
                                            .map(inpSource -> (LocalInputSource) inpSource)
                                            .map(LocalInputSource::getFiles)
                                            .flatMap(List::stream)
                                            .collect(Collectors.toList());

    Assert.assertEquals(1, inputSource.estimateNumSplits(null, new MaxSizeSplitHintSpec(1L, null)));
    Assert.assertEquals(1, localInputSourceList.size());
    CloseableIterable<Record> datafileReader = Parquet.read(Files.localInput(localInputSourceList.get(0)))
                                                      .project(tableSchema)
                                                      .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(
                                                          tableSchema,
                                                          fileSchema
                                                      ))
                                                      .build();


    for (Record record : datafileReader) {
      Assert.assertEquals(tableData.get("id"), record.get(0));
      Assert.assertEquals(tableData.get("name"), record.get(1));
    }
  }

  @Test
  public void testInputSourceReadFromLatestSnapshot() throws IOException
  {
    IcebergInputSource inputSource = new IcebergInputSource(
        TABLENAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        DateTimes.nowUtc(),
        null
    );
    Stream<InputSplit<List<String>>> splits = inputSource.createSplits(null, new MaxSizeSplitHintSpec(null, null));
    Assert.assertEquals(1, splits.count());
  }

  @Test
  public void testCaseInsensitiveFiltering() throws IOException
  {
    LocalCatalog caseInsensitiveCatalog = new LocalCatalog(warehouseDir.getPath(), new HashMap<>(), false);
    Table icebergTableFromSchema = testCatalog.retrieveCatalog().loadTable(tableIdentifier);

    icebergTableFromSchema.updateSchema().renameColumn("name", "Name").commit();
    IcebergInputSource inputSource = new IcebergInputSource(
        TABLENAME,
        NAMESPACE,
        new IcebergEqualsFilter("name", "Foo"),
        caseInsensitiveCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    Stream<InputSplit<List<String>>> splits = inputSource.createSplits(null, new MaxSizeSplitHintSpec(null, null));
    List<File> localInputSourceList = splits.map(inputSource::withSplit)
                                            .map(inpSource -> (LocalInputSource) inpSource)
                                            .map(LocalInputSource::getFiles)
                                            .flatMap(List::stream)
                                            .collect(Collectors.toList());

    Assert.assertEquals(1, inputSource.estimateNumSplits(null, new MaxSizeSplitHintSpec(1L, null)));
    Assert.assertEquals(1, localInputSourceList.size());
  }

  @Test
  public void testResidualFilterModeIgnore() throws IOException
  {
    // Filter on non-partition column with IGNORE mode should succeed
    IcebergInputSource inputSource = new IcebergInputSource(
        TABLENAME,
        NAMESPACE,
        new IcebergEqualsFilter("id", "123988"),
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        ResidualFilterMode.IGNORE
    );
    Stream<InputSplit<List<String>>> splits = inputSource.createSplits(null, new MaxSizeSplitHintSpec(null, null));
    Assert.assertEquals(1, splits.count());
  }

  @Test
  public void testResidualFilterModeFail() throws IOException
  {
    // Filter on non-partition column with FAIL mode should throw exception
    IcebergInputSource inputSource = new IcebergInputSource(
        TABLENAME,
        NAMESPACE,
        new IcebergEqualsFilter("id", "123988"),
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        ResidualFilterMode.FAIL
    );
    DruidException exception = Assert.assertThrows(
        DruidException.class,
        () -> inputSource.createSplits(null, new MaxSizeSplitHintSpec(null, null))
    );
    Assert.assertTrue(
        "Expect residual error to be thrown",
        exception.getMessage().contains("residual")
    );
  }

  @Test
  public void testResidualFilterModeFailWithPartitionedTable() throws IOException
  {
    // Cleanup default table first
    tearDown();
    // Create a partitioned table and filter on the partition column
    tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), "partitionedTable");
    createAndLoadPartitionedTable(tableIdentifier);

    IcebergInputSource inputSource = new IcebergInputSource(
        "partitionedTable",
        NAMESPACE,
        new IcebergEqualsFilter("id", "123988"),
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        ResidualFilterMode.FAIL
    );
    Stream<InputSplit<List<String>>> splits = inputSource.createSplits(null, new MaxSizeSplitHintSpec(null, null));
    Assert.assertEquals(1, splits.count());
  }

  @Test
  public void testResidualFilterModeFailWithPartitionedTableNonPartitionColumn() throws IOException
  {
    // Cleanup default table first
    tearDown();
    // Create a partitioned table and filter on a non-partition column
    tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), "partitionedTable2");
    createAndLoadPartitionedTable(tableIdentifier);

    // Filter on non-partition column with FAIL mode should throw exception
    IcebergInputSource inputSource = new IcebergInputSource(
        "partitionedTable2",
        NAMESPACE,
        new IcebergEqualsFilter("name", "Foo"),
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        ResidualFilterMode.FAIL
    );
    DruidException exception = Assert.assertThrows(
        DruidException.class,
        () -> inputSource.createSplits(null, new MaxSizeSplitHintSpec(null, null))
    );
    Assert.assertTrue(
        "Expect residual error to be thrown",
        exception.getMessage().contains("residual")
    );
  }

  @Test
  public void testInputSourceWithColumnProjection() throws IOException
  {
    IcebergInputSource inputSource = new IcebergInputSource(
        TABLENAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );
    Stream<InputSplit<List<String>>> splits = inputSource.createSplits(null, new MaxSizeSplitHintSpec(null, null));
    List<File> localInputSourceList = splits.map(inputSource::withSplit)
                                            .map(inpSource -> (LocalInputSource) inpSource)
                                            .map(LocalInputSource::getFiles)
                                            .flatMap(List::stream)
                                            .collect(Collectors.toList());

    Assert.assertEquals(1, inputSource.estimateNumSplits(null, new MaxSizeSplitHintSpec(1L, null)));
    Assert.assertEquals(1, localInputSourceList.size());

    // First, read all columns without projection
    List<Record> allRecords = new ArrayList<>();
    CloseableIterable<Record> fullDataReader = Parquet.read(Files.localInput(localInputSourceList.get(0)))
                                                      .project(tableSchema)
                                                      .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(
                                                          tableSchema,
                                                          fileSchema
                                                      ))
                                                      .build();

    for (Record record : fullDataReader) {
      allRecords.add(record);
    }
    fullDataReader.close();

    Assert.assertEquals(1, allRecords.size());
    Record fullRecord = allRecords.get(0);
    Assert.assertEquals(2, fullRecord.size()); // Should have 2 columns
    Assert.assertEquals(tableData.get("id"), fullRecord.get(0));
    Assert.assertEquals(tableData.get("name"), fullRecord.get(1));

    // Read with projection - only 'id' column

    // Create schema with only the 'id' column for projection
    Schema idProjectedSchema = new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get())
    );

    List<Record> idProjectedRecords = new ArrayList<>();
    CloseableIterable<Record> idProjectedReader = Parquet.read(Files.localInput(localInputSourceList.get(0)))
                                                         .project(idProjectedSchema)
                                                         .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(
                                                             idProjectedSchema,
                                                             fileSchema
                                                         ))
                                                         .build();

    for (Record record : idProjectedReader) {
      idProjectedRecords.add(record);
    }
    idProjectedReader.close();

    Assert.assertEquals(1, idProjectedRecords.size());
    Record idRecord = idProjectedRecords.get(0);
    Assert.assertEquals(1, idRecord.size()); // Should have only 1 column
    Assert.assertEquals(fullRecord.get(0), idRecord.get(0)); // Id should match full record

    // Read with projection - only 'name' column

    // Create schema with only the 'name' column for projection
    Schema nameProjectedSchema = new Schema(
            Types.NestedField.required(2, "name", Types.StringType.get())
    );

    List<Record> nameProjectedRecords = new ArrayList<>();
    CloseableIterable<Record> nameProjectedReader = Parquet.read(Files.localInput(localInputSourceList.get(0)))
                                                           .project(nameProjectedSchema)
                                                           .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(
                                                               nameProjectedSchema,
                                                               fileSchema
                                                           ))
                                                           .build();

    for (Record record : nameProjectedReader) {
      nameProjectedRecords.add(record);
    }
    nameProjectedReader.close();

    Assert.assertEquals(1, nameProjectedRecords.size());
    Record nameRecord = nameProjectedRecords.get(0);
    Assert.assertEquals(1, nameRecord.size()); // Should have only 1 column
    Assert.assertEquals(fullRecord.get(1), nameRecord.get(0)); // Name should match full record
  }

  @After
  public void tearDown()
  {
    dropTableFromCatalog(tableIdentifier);
  }

  private void createAndLoadTable(TableIdentifier tableIdentifier) throws IOException
  {
    //Setup iceberg table and schema
    Table icebergTableFromSchema = testCatalog.retrieveCatalog().createTable(tableIdentifier, tableSchema);
    //Generate an iceberg record and write it to a file
    GenericRecord record = GenericRecord.create(tableSchema);
    ImmutableList.Builder<GenericRecord> builder = ImmutableList.builder();

    builder.add(record.copy(tableData));
    String filepath = icebergTableFromSchema.location() + "/" + UUID.randomUUID();
    OutputFile file = icebergTableFromSchema.io().newOutputFile(filepath);
    DataWriter<GenericRecord> dataWriter =
        Parquet.writeData(file)
               .schema(tableSchema)
               .createWriterFunc(GenericParquetWriter::create)
               .overwrite()
               .withSpec(PartitionSpec.unpartitioned())
               .build();

    try {
      for (GenericRecord genRecord : builder.build()) {
        dataWriter.write(genRecord);
      }
    }
    finally {
      dataWriter.close();
    }
    DataFile dataFile = dataWriter.toDataFile();

    //Add the data file to the iceberg table
    icebergTableFromSchema.newAppend().appendFile(dataFile).commit();

  }

  private void createAndLoadPartitionedTable(TableIdentifier tableIdentifier) throws IOException
  {
    // Create a partitioned table with 'id' as the partition column
    PartitionSpec partitionSpec = PartitionSpec.builderFor(tableSchema)
                                               .identity("id")
                                               .build();
    Table icebergTable = testCatalog.retrieveCatalog().createTable(tableIdentifier, tableSchema, partitionSpec);

    // Generate an iceberg record and write it to a file
    GenericRecord record = GenericRecord.create(tableSchema);
    ImmutableList.Builder<GenericRecord> builder = ImmutableList.builder();

    builder.add(record.copy(tableData));
    String filepath = icebergTable.location() + "/data/id=123988/" + UUID.randomUUID() + ".parquet";
    OutputFile file = icebergTable.io().newOutputFile(filepath);

    // Create a partition key for the partition spec
    PartitionKey partitionKey = new PartitionKey(partitionSpec, tableSchema);
    partitionKey.partition(record.copy(tableData));

    DataWriter<GenericRecord> dataWriter =
        Parquet.writeData(file)
               .schema(tableSchema)
               .createWriterFunc(GenericParquetWriter::create)
               .overwrite()
               .withSpec(partitionSpec)
               .withPartition(partitionKey)
               .build();

    try {
      for (GenericRecord genRecord : builder.build()) {
        dataWriter.write(genRecord);
      }
    }
    finally {
      dataWriter.close();
    }
    DataFile dataFile = dataWriter.toDataFile();

    // Add the data file to the iceberg table
    icebergTable.newAppend().appendFile(dataFile).commit();
  }

  private void dropTableFromCatalog(TableIdentifier tableIdentifier)
  {
    testCatalog.retrieveCatalog().dropTable(tableIdentifier);
  }

}
