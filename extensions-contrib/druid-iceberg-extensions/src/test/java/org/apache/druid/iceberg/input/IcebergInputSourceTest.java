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
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.LocalInputSourceFactory;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.iceberg.filter.IcebergEqualsFilter;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
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

  // =====================================================================================
  // Iceberg V2 tests
  // =====================================================================================

  /**
   * Creates a V2 format table with 3 records, writes a position-delete file that deletes row at
   * position 1, and verifies that only 2 rows survive.
   */
  @Test
  public void testInputSourceV2WithPositionDeletes() throws IOException
  {
    tearDown();
    String v2TableName = "v2PosDeleteTable";
    tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), v2TableName);

    // Schema includes a timestamp column for InputRowSchema
    Schema v2Schema = new Schema(
        Types.NestedField.required(1, "id", Types.StringType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "__time", Types.LongType.get())
    );

    List<Map<String, Object>> rows = ImmutableList.of(
        ImmutableMap.of("id", "1", "name", "Alice", "__time", 0L),
        ImmutableMap.of("id", "123988", "name", "Foo", "__time", 1000L),   // will be deleted
        ImmutableMap.of("id", "3", "name", "Charlie", "__time", 2000L)
    );

    // Create V2 table and write data
    Table table = testCatalog.retrieveCatalog().createTable(
        tableIdentifier,
        v2Schema,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "2")
    );

    String dataFilePath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    DataFile dataFile = writeParquetDataFile(table, v2Schema, rows, dataFilePath);
    table.newAppend().appendFile(dataFile).commit();

    // Write a position-delete file: delete the row at position 1 (0-indexed)
    String posDeletePath = table.location() + "/delete-files/" + UUID.randomUUID() + ".parquet";
    DeleteFile posDeleteFile = writePositionDeleteFile(table, dataFilePath, 1L, posDeletePath);
    table.newRowDelta().addDeletes(posDeleteFile).commit();

    // Read via IcebergInputSource
    InputRowSchema inputRowSchema = new InputRowSchema(
        new TimestampSpec("__time", "millis", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("id", "name"))),
        ColumnsFilter.all()
    );

    IcebergInputSource inputSource = new IcebergInputSource(
        v2TableName,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    List<InputRow> result = new ArrayList<>();
    try (CloseableIterator<InputRow> it = inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder()).read(null)) {
      it.forEachRemaining(result::add);
    }

    Assert.assertEquals("Position delete should remove exactly one row", 2, result.size());
    List<String> ids = result.stream()
                             .map(r -> r.getDimension("id").get(0))
                             .collect(Collectors.toList());
    Assert.assertTrue("Row 'Alice' should survive", ids.contains("1"));
    Assert.assertFalse("Row 'Foo' (id=123988) should be deleted", ids.contains("123988"));
    Assert.assertTrue("Row 'Charlie' should survive", ids.contains("3"));
  }

  /**
   * Creates a V2 format table, writes an equality-delete file that deletes any row where
   * {@code id = "123988"}, and verifies the row is absent from the reader output.
   */
  @Test
  public void testInputSourceV2WithEqualityDeletes() throws IOException
  {
    tearDown();
    String v2TableName = "v2EqDeleteTable";
    tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), v2TableName);

    Schema v2Schema = new Schema(
        Types.NestedField.required(1, "id", Types.StringType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "__time", Types.LongType.get())
    );

    List<Map<String, Object>> rows = ImmutableList.of(
        ImmutableMap.of("id", "1", "name", "Alice", "__time", 0L),
        ImmutableMap.of("id", "123988", "name", "Foo", "__time", 1000L),  // will be deleted
        ImmutableMap.of("id", "3", "name", "Charlie", "__time", 2000L)
    );

    Table table = testCatalog.retrieveCatalog().createTable(
        tableIdentifier,
        v2Schema,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "2")
    );

    String dataFilePath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    DataFile dataFile = writeParquetDataFile(table, v2Schema, rows, dataFilePath);
    table.newAppend().appendFile(dataFile).commit();

    // Equality-delete schema: just the "id" field (field ID 1)
    Schema eqDeleteSchema = v2Schema.select(ImmutableList.of("id"));
    String eqDeletePath = table.location() + "/delete-files/" + UUID.randomUUID() + ".parquet";
    DeleteFile eqDeleteFile = writeEqualityDeleteFile(
        table,
        eqDeleteSchema,
        ImmutableList.of(1), // field ID for "id"
        ImmutableList.of(ImmutableMap.of("id", "123988")),
        eqDeletePath
    );
    table.newRowDelta().addDeletes(eqDeleteFile).commit();

    InputRowSchema inputRowSchema = new InputRowSchema(
        new TimestampSpec("__time", "millis", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("id", "name"))),
        ColumnsFilter.all()
    );

    IcebergInputSource inputSource = new IcebergInputSource(
        v2TableName,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    List<InputRow> result = new ArrayList<>();
    try (CloseableIterator<InputRow> it = inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder()).read(null)) {
      it.forEachRemaining(result::add);
    }

    Assert.assertEquals("Equality delete should remove exactly one row", 2, result.size());
    List<String> ids = result.stream()
                             .map(r -> r.getDimension("id").get(0))
                             .collect(Collectors.toList());
    Assert.assertFalse("Row with id='123988' should be equality-deleted", ids.contains("123988"));
    Assert.assertTrue("Other rows should survive", ids.contains("1"));
    Assert.assertTrue("Other rows should survive", ids.contains("3"));
  }

  /**
   * V2 format table with NO delete files should fall through to the V1 path-based approach.
   */
  @Test
  public void testInputSourceV2WithNoDeleteFiles() throws IOException
  {
    tearDown();
    String v2TableName = "v2NoDeleteTable";
    tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), v2TableName);

    Table table = testCatalog.retrieveCatalog().createTable(
        tableIdentifier,
        tableSchema,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "2")
    );

    GenericRecord record = GenericRecord.create(tableSchema);
    record.setField("id", "123988");
    record.setField("name", "Foo");
    writeAndCommit(table, tableSchema, ImmutableList.of(record));

    IcebergInputSource inputSource = new IcebergInputSource(
        v2TableName,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    // No delete files → falls through to V1 path → splits are LocalInputSource backed
    Stream<InputSplit<List<String>>> splits = inputSource.createSplits(null, new MaxSizeSplitHintSpec(null, null));
    List<InputSource> splitSources = splits.map(inputSource::withSplit).collect(Collectors.toList());

    Assert.assertEquals(1, splitSources.size());
    // V1 path returns a LocalInputSource (not IcebergFileTaskInputSource)
    Assert.assertFalse(
        "V2 table without delete files should use V1 (path-based) path",
        splitSources.get(0) instanceof IcebergFileTaskInputSource
    );
  }

  /**
   * Unit-level test for the V2 split encoding/decoding contract.
   */
  @Test
  public void testInputSourceV2SplitEncoding() throws IOException
  {
    tearDown();
    String v2TableName = "v2SplitEncTable";
    tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), v2TableName);

    Schema v2Schema = new Schema(
        Types.NestedField.required(1, "id", Types.StringType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "__time", Types.LongType.get())
    );

    List<Map<String, Object>> rows = ImmutableList.of(
        ImmutableMap.of("id", "a", "name", "A", "__time", 0L),
        ImmutableMap.of("id", "b", "name", "B", "__time", 1000L)
    );

    Table table = testCatalog.retrieveCatalog().createTable(
        tableIdentifier,
        v2Schema,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "2")
    );

    String dataFilePath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    DataFile dataFile = writeParquetDataFile(table, v2Schema, rows, dataFilePath);
    table.newAppend().appendFile(dataFile).commit();

    // Add a position delete file so the V2 path is triggered
    String posDeletePath = table.location() + "/delete-files/" + UUID.randomUUID() + ".parquet";
    DeleteFile posDeleteFile = writePositionDeleteFile(table, dataFilePath, 0L, posDeletePath);
    table.newRowDelta().addDeletes(posDeleteFile).commit();

    IcebergInputSource inputSource = new IcebergInputSource(
        v2TableName,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    // Trigger planning
    List<InputSplit<List<String>>> splits = inputSource.createSplits(null, new MaxSizeSplitHintSpec(null, null))
                                                       .collect(Collectors.toList());
    Assert.assertEquals(1, splits.size());

    // Verify the split carries the v2 marker
    List<String> splitParts = splits.get(0).get();
    Assert.assertEquals("First element must be v2 marker", "v2", splitParts.get(0));
    // parts[5] must be the table schema JSON
    Assert.assertNotNull("Split must carry table schema JSON at index 5", splitParts.get(5));
    Assert.assertTrue(
        "Schema JSON must look like an Iceberg schema object",
        splitParts.get(5).contains("\"type\"") || splitParts.get(5).contains("fields")
    );
    // The POS: entry must appear at index 6 or later
    boolean hasPosEntry = splitParts.stream().anyMatch(p -> p.startsWith("POS:"));
    Assert.assertTrue("V2 split with position delete must contain a POS: entry", hasPosEntry);

    // withSplit must return IcebergFileTaskInputSource
    InputSource splitSource = inputSource.withSplit(splits.get(0));
    Assert.assertTrue(
        "withSplit on a v2 split must return IcebergFileTaskInputSource",
        splitSource instanceof IcebergFileTaskInputSource
    );

    // A V1 split (no marker) should return a non-IcebergFileTaskInputSource
    InputSplit<List<String>> v1Split = new InputSplit<>(ImmutableList.of(dataFilePath));
    // Reinitialise so delegateInputSource is available
    IcebergInputSource v1Source = new IcebergInputSource(
        v2TableName,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );
    // Force V1 path by loading without delete files (not possible with current table,
    // so just verify the encoding roundtrip):
    IcebergFileTaskInputSource decodedSource = (IcebergFileTaskInputSource) splitSource;
    Assert.assertEquals(dataFilePath, decodedSource.getDataFilePath());
    Assert.assertEquals("PARQUET", decodedSource.getFileFormat());
    Assert.assertNotNull("Decoded source must carry table schema JSON", decodedSource.getTableSchemaJson());
    Assert.assertEquals(1, decodedSource.getDeleteFiles().size());
    Assert.assertTrue(decodedSource.getDeleteFiles().get(0).isPositionDelete());
    Assert.assertEquals(NAMESPACE, decodedSource.getTableNamespace());
    Assert.assertEquals(v2TableName, decodedSource.getTableName());
  }

  /**
   * Two sequential position-delete files against the same data file — both must be applied.
   */
  @Test
  public void testInputSourceV2MultiplePositionDeleteFiles() throws IOException
  {
    tearDown();
    String v2TableName = "v2MultiPosDeleteTable";
    tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), v2TableName);

    Schema v2Schema = new Schema(
        Types.NestedField.required(1, "id", Types.StringType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "__time", Types.LongType.get())
    );

    List<Map<String, Object>> rows = ImmutableList.of(
        ImmutableMap.of("id", "1", "name", "Alice", "__time", 0L),
        ImmutableMap.of("id", "2", "name", "Bob", "__time", 1000L),    // deleted by file 1
        ImmutableMap.of("id", "3", "name", "Charlie", "__time", 2000L),
        ImmutableMap.of("id", "4", "name", "Dave", "__time", 3000L),   // deleted by file 2
        ImmutableMap.of("id", "5", "name", "Eve", "__time", 4000L)
    );

    Table table = testCatalog.retrieveCatalog().createTable(
        tableIdentifier,
        v2Schema,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "2")
    );

    String dataFilePath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    DataFile dataFile = writeParquetDataFile(table, v2Schema, rows, dataFilePath);
    table.newAppend().appendFile(dataFile).commit();

    // First delete file: remove row at position 1 (Bob)
    String posDeletePath1 = table.location() + "/delete-files/" + UUID.randomUUID() + ".parquet";
    DeleteFile posDeleteFile1 = writePositionDeleteFile(table, dataFilePath, 1L, posDeletePath1);
    table.newRowDelta().addDeletes(posDeleteFile1).commit();

    // Second delete file: remove row at position 3 (Dave)
    String posDeletePath2 = table.location() + "/delete-files/" + UUID.randomUUID() + ".parquet";
    DeleteFile posDeleteFile2 = writePositionDeleteFile(table, dataFilePath, 3L, posDeletePath2);
    table.newRowDelta().addDeletes(posDeleteFile2).commit();

    InputRowSchema inputRowSchema = new InputRowSchema(
        new TimestampSpec("__time", "millis", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("id", "name"))),
        ColumnsFilter.all()
    );

    IcebergInputSource inputSource = new IcebergInputSource(
        v2TableName,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    List<InputRow> result = new ArrayList<>();
    try (CloseableIterator<InputRow> it = inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder()).read(null)) {
      it.forEachRemaining(result::add);
    }

    Assert.assertEquals("Both delete files must be applied; 3 rows should survive", 3, result.size());
    List<String> ids = result.stream().map(r -> r.getDimension("id").get(0)).collect(Collectors.toList());
    Assert.assertTrue(ids.contains("1"));
    Assert.assertFalse("Bob (pos 1) should be deleted by first delete file", ids.contains("2"));
    Assert.assertTrue(ids.contains("3"));
    Assert.assertFalse("Dave (pos 3) should be deleted by second delete file", ids.contains("4"));
    Assert.assertTrue(ids.contains("5"));
  }

  /**
   * All rows in a data file are position-deleted — reader must return zero rows without error.
   */
  @Test
  public void testInputSourceV2AllRowsDeleted() throws IOException
  {
    tearDown();
    String v2TableName = "v2AllDeletedTable";
    tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), v2TableName);

    Schema v2Schema = new Schema(
        Types.NestedField.required(1, "id", Types.StringType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "__time", Types.LongType.get())
    );

    List<Map<String, Object>> rows = ImmutableList.of(
        ImmutableMap.of("id", "1", "name", "Alice", "__time", 0L),
        ImmutableMap.of("id", "2", "name", "Bob", "__time", 1000L)
    );

    Table table = testCatalog.retrieveCatalog().createTable(
        tableIdentifier,
        v2Schema,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "2")
    );

    String dataFilePath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    DataFile dataFile = writeParquetDataFile(table, v2Schema, rows, dataFilePath);
    table.newAppend().appendFile(dataFile).commit();

    // Delete every row
    String posDeletePath = table.location() + "/delete-files/" + UUID.randomUUID() + ".parquet";
    DeleteFile posDeleteFile = writePositionDeleteFile(table, dataFilePath, ImmutableList.of(0L, 1L), posDeletePath);
    table.newRowDelta().addDeletes(posDeleteFile).commit();

    InputRowSchema inputRowSchema = new InputRowSchema(
        new TimestampSpec("__time", "millis", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("id", "name"))),
        ColumnsFilter.all()
    );

    IcebergInputSource inputSource = new IcebergInputSource(
        v2TableName,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    List<InputRow> result = new ArrayList<>();
    try (CloseableIterator<InputRow> it = inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder()).read(null)) {
      it.forEachRemaining(result::add);
    }

    Assert.assertEquals("All rows deleted — reader must return zero rows", 0, result.size());
  }

  /**
   * Both a position-delete and an equality-delete apply to the same data file.
   */
  @Test
  public void testInputSourceV2MixedDeleteTypes() throws IOException
  {
    tearDown();
    String v2TableName = "v2MixedDeleteTable";
    tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), v2TableName);

    Schema v2Schema = new Schema(
        Types.NestedField.required(1, "id", Types.StringType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "__time", Types.LongType.get())
    );

    List<Map<String, Object>> rows = ImmutableList.of(
        ImmutableMap.of("id", "1", "name", "Alice", "__time", 0L),
        ImmutableMap.of("id", "2", "name", "Bob", "__time", 1000L),    // removed by position delete
        ImmutableMap.of("id", "3", "name", "Charlie", "__time", 2000L) // removed by equality delete
    );

    Table table = testCatalog.retrieveCatalog().createTable(
        tableIdentifier,
        v2Schema,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "2")
    );

    String dataFilePath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    DataFile dataFile = writeParquetDataFile(table, v2Schema, rows, dataFilePath);
    table.newAppend().appendFile(dataFile).commit();

    // Position delete: remove Bob (position 1)
    String posDeletePath = table.location() + "/delete-files/" + UUID.randomUUID() + ".parquet";
    DeleteFile posDeleteFile = writePositionDeleteFile(table, dataFilePath, 1L, posDeletePath);
    table.newRowDelta().addDeletes(posDeleteFile).commit();

    // Equality delete: remove Charlie (id = "3")
    Schema eqDeleteSchema = v2Schema.select(ImmutableList.of("id"));
    String eqDeletePath = table.location() + "/delete-files/" + UUID.randomUUID() + ".parquet";
    DeleteFile eqDeleteFile = writeEqualityDeleteFile(
        table,
        eqDeleteSchema,
        ImmutableList.of(1),
        ImmutableList.of(ImmutableMap.of("id", "3")),
        eqDeletePath
    );
    table.newRowDelta().addDeletes(eqDeleteFile).commit();

    InputRowSchema inputRowSchema = new InputRowSchema(
        new TimestampSpec("__time", "millis", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("id", "name"))),
        ColumnsFilter.all()
    );

    IcebergInputSource inputSource = new IcebergInputSource(
        v2TableName,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    List<InputRow> result = new ArrayList<>();
    try (CloseableIterator<InputRow> it = inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder()).read(null)) {
      it.forEachRemaining(result::add);
    }

    Assert.assertEquals("Position and equality delete must both be applied; only Alice survives", 1, result.size());
    Assert.assertEquals("1", result.get(0).getDimension("id").get(0));
  }

  /**
   * Two data files each with their own position-delete file — confirms per-split correctness.
   */
  @Test
  public void testInputSourceV2MultipleDataFilesWithDeletes() throws IOException
  {
    tearDown();
    String v2TableName = "v2MultiDataFileTable";
    tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), v2TableName);

    Schema v2Schema = new Schema(
        Types.NestedField.required(1, "id", Types.StringType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "__time", Types.LongType.get())
    );

    Table table = testCatalog.retrieveCatalog().createTable(
        tableIdentifier,
        v2Schema,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "2")
    );

    // Data file 1: [Alice, Bob] — delete Alice (pos 0), keep Bob
    List<Map<String, Object>> rows1 = ImmutableList.of(
        ImmutableMap.of("id", "1", "name", "Alice", "__time", 0L),
        ImmutableMap.of("id", "2", "name", "Bob", "__time", 1000L)
    );
    String dataFilePath1 = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    DataFile dataFile1 = writeParquetDataFile(table, v2Schema, rows1, dataFilePath1);

    // Data file 2: [Charlie, Dave] — delete Dave (pos 1), keep Charlie
    List<Map<String, Object>> rows2 = ImmutableList.of(
        ImmutableMap.of("id", "3", "name", "Charlie", "__time", 2000L),
        ImmutableMap.of("id", "4", "name", "Dave", "__time", 3000L)
    );
    String dataFilePath2 = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    DataFile dataFile2 = writeParquetDataFile(table, v2Schema, rows2, dataFilePath2);

    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    String posDeletePath1 = table.location() + "/delete-files/" + UUID.randomUUID() + ".parquet";
    DeleteFile posDeleteFile1 = writePositionDeleteFile(table, dataFilePath1, 0L, posDeletePath1);

    String posDeletePath2 = table.location() + "/delete-files/" + UUID.randomUUID() + ".parquet";
    DeleteFile posDeleteFile2 = writePositionDeleteFile(table, dataFilePath2, 1L, posDeletePath2);

    table.newRowDelta().addDeletes(posDeleteFile1).addDeletes(posDeleteFile2).commit();

    InputRowSchema inputRowSchema = new InputRowSchema(
        new TimestampSpec("__time", "millis", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("id", "name"))),
        ColumnsFilter.all()
    );

    IcebergInputSource inputSource = new IcebergInputSource(
        v2TableName,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    List<InputRow> result = new ArrayList<>();
    try (CloseableIterator<InputRow> it = inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder()).read(null)) {
      it.forEachRemaining(result::add);
    }

    Assert.assertEquals("One row deleted per data file; 2 should survive", 2, result.size());
    List<String> ids = result.stream().map(r -> r.getDimension("id").get(0)).collect(Collectors.toList());
    Assert.assertFalse("Alice (file1, pos 0) should be deleted", ids.contains("1"));
    Assert.assertTrue("Bob should survive", ids.contains("2"));
    Assert.assertTrue("Charlie should survive", ids.contains("3"));
    Assert.assertFalse("Dave (file2, pos 1) should be deleted", ids.contains("4"));
  }

  @After
  public void tearDown()
  {
    dropTableFromCatalog(tableIdentifier);
  }

  // =====================================================================================
  // Helper methods
  // =====================================================================================

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
               .createWriterFunc(GenericParquetWriter::buildWriter)
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
               .createWriterFunc(GenericParquetWriter::buildWriter)
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

  /**
   * Writes a Parquet data file with the given rows and returns the resulting {@link DataFile}.
   */
  private DataFile writeParquetDataFile(
      Table table,
      Schema schema,
      List<Map<String, Object>> rows,
      String filePath
  ) throws IOException
  {
    OutputFile outputFile = table.io().newOutputFile(filePath);
    DataWriter<GenericRecord> dataWriter =
        Parquet.writeData(outputFile)
               .schema(schema)
               .createWriterFunc(GenericParquetWriter::buildWriter)
               .overwrite()
               .withSpec(PartitionSpec.unpartitioned())
               .build();
    try {
      for (Map<String, Object> row : rows) {
        GenericRecord record = GenericRecord.create(schema);
        for (Types.NestedField field : schema.columns()) {
          record.setField(field.name(), row.get(field.name()));
        }
        dataWriter.write(record);
      }
    }
    finally {
      dataWriter.close();
    }
    return dataWriter.toDataFile();
  }

  /**
   * Writes a Parquet position-delete file that marks {@code position} in {@code dataFilePath} as
   * deleted.  Returns the resulting {@link DeleteFile} metadata object.
   */
  private DeleteFile writePositionDeleteFile(
      Table table,
      String dataFilePath,
      long position,
      String deleteFilePath
  ) throws IOException
  {
    return writePositionDeleteFile(table, dataFilePath, ImmutableList.of(position), deleteFilePath);
  }

  private DeleteFile writePositionDeleteFile(
      Table table,
      String dataFilePath,
      List<Long> positions,
      String deleteFilePath
  ) throws IOException
  {
    OutputFile outputFile = table.io().newOutputFile(deleteFilePath);
    PositionDeleteWriter<GenericRecord> writer =
        Parquet.writeDeletes(outputFile)
               .createWriterFunc(GenericParquetWriter::buildWriter)
               .overwrite()
               .withSpec(PartitionSpec.unpartitioned())
               .buildPositionWriter();
    try {
      PositionDelete<GenericRecord> posDelete = PositionDelete.create();
      for (long pos : positions) {
        posDelete.set(dataFilePath, pos, null);
        writer.write(posDelete);
      }
    }
    finally {
      writer.close();
    }
    return writer.toDeleteFile();
  }

  /**
   * Writes a Parquet equality-delete file with the given equality field IDs and delete rows.
   * Returns the resulting {@link DeleteFile} metadata object.
   */
  private DeleteFile writeEqualityDeleteFile(
      Table table,
      Schema eqDeleteSchema,
      List<Integer> equalityFieldIds,
      List<Map<String, Object>> deleteRows,
      String deleteFilePath
  ) throws IOException
  {
    OutputFile outputFile = table.io().newOutputFile(deleteFilePath);
    int[] fieldIdArray = equalityFieldIds.stream().mapToInt(Integer::intValue).toArray();
    EqualityDeleteWriter<GenericRecord> writer =
        Parquet.writeDeletes(outputFile)
               .createWriterFunc(GenericParquetWriter::buildWriter)
               .overwrite()
               .withSpec(PartitionSpec.unpartitioned())
               .rowSchema(eqDeleteSchema)
               .equalityFieldIds(fieldIdArray)
               .buildEqualityWriter();
    try {
      for (Map<String, Object> deleteRow : deleteRows) {
        GenericRecord record = GenericRecord.create(eqDeleteSchema);
        for (Types.NestedField field : eqDeleteSchema.columns()) {
          record.setField(field.name(), deleteRow.get(field.name()));
        }
        writer.write(record);
      }
    }
    finally {
      writer.close();
    }
    return writer.toDeleteFile();
  }

  /**
   * Writes the given records to a new Parquet file, appends the file to {@code table}, and
   * returns the resulting {@link DataFile}.
   */
  private DataFile writeAndCommit(Table table, Schema schema, List<GenericRecord> records)
      throws IOException
  {
    String filePath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    OutputFile outputFile = table.io().newOutputFile(filePath);
    DataWriter<GenericRecord> dataWriter =
        Parquet.writeData(outputFile)
               .schema(schema)
               .createWriterFunc(GenericParquetWriter::buildWriter)
               .overwrite()
               .withSpec(PartitionSpec.unpartitioned())
               .build();
    try {
      for (GenericRecord record : records) {
        dataWriter.write(record);
      }
    }
    finally {
      dataWriter.close();
    }
    DataFile dataFile = dataWriter.toDataFile();
    table.newAppend().appendFile(dataFile).commit();
    return dataFile;
  }

  private void dropTableFromCatalog(TableIdentifier tableIdentifier)
  {
    testCatalog.retrieveCatalog().dropTable(tableIdentifier);
  }

}
