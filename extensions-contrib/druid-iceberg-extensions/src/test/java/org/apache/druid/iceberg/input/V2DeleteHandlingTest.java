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
import org.apache.druid.data.input.FilePerSplitHintSpec;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSourceFactory;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Tests for automatic Iceberg v2 delete file detection and application.
 * Verifies that IcebergInputSource transparently handles positional and
 * equality deletes without any user configuration.
 */
public class V2DeleteHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private LocalCatalog testCatalog;
  private File warehouseDir;

  private static final String NAMESPACE = "default";
  private static final String TABLE_NAME = "v2TestTable";

  private final Schema tableSchema = new Schema(
      Types.NestedField.required(1, "order_id", Types.IntegerType.get()),
      Types.NestedField.required(2, "product", Types.StringType.get()),
      Types.NestedField.required(3, "amount", Types.DoubleType.get())
  );

  private final InputRowSchema inputRowSchema = new InputRowSchema(
      new TimestampSpec("order_id", "auto", null),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("order_id", "product", "amount"))),
      ColumnsFilter.all(),
      ImmutableSet.of()
  );

  @Before
  public void setup()
  {
    warehouseDir = FileUtils.createTempDir();
    testCatalog = new LocalCatalog(warehouseDir.getPath(), new HashMap<>(), true);
  }

  @After
  public void tearDown()
  {
    final TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), TABLE_NAME);
    try {
      testCatalog.retrieveCatalog().dropTable(tableId);
    }
    catch (Exception e) {
      // ignore
    }
  }

  @Test
  public void testAutoDetectWithEqualityDelete() throws IOException
  {
    createTableWithEqualityDelete();

    final IcebergInputSource inputSource = new IcebergInputSource(
        TABLE_NAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    final InputSourceReader reader = inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder());
    final List<InputRow> rows = readAll(reader);

    // Equality delete removed order_id=2, so only 2 rows remain
    Assert.assertEquals(2, rows.size());

    final List<String> orderIds = new ArrayList<>();
    for (final InputRow row : rows) {
      orderIds.add(row.getDimension("order_id").get(0));
    }
    Assert.assertTrue("Should contain order_id 1", orderIds.contains("1"));
    Assert.assertTrue("Should contain order_id 3", orderIds.contains("3"));
    Assert.assertFalse("Should NOT contain deleted order_id 2", orderIds.contains("2"));
  }

  @Test
  public void testAutoDetectWithPositionalDelete() throws IOException
  {
    createTableWithPositionalDelete();

    final IcebergInputSource inputSource = new IcebergInputSource(
        TABLE_NAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    final InputSourceReader reader = inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder());
    final List<InputRow> rows = readAll(reader);

    // Positional delete removed row at position 1 (order_id=2), so 2 rows remain
    Assert.assertEquals(2, rows.size());
  }

  @Test
  public void testV1TableWithNoDeletesUsesFilePaths() throws IOException
  {
    createTableWithoutDeletes();

    // Verify scan detects no deletes
    final IcebergCatalog.FileScanResult result = testCatalog.extractFileScanTasks(
        NAMESPACE,
        TABLE_NAME,
        null,
        null,
        ResidualFilterMode.IGNORE
    );

    Assert.assertFalse("V1 table should have no delete files", result.hasDeleteFiles());
    Assert.assertEquals(1, result.getFileScanTasks().size());
  }

  @Test
  public void testV2TableDeleteDetection() throws IOException
  {
    createTableWithEqualityDelete();

    final IcebergCatalog.FileScanResult result = testCatalog.extractFileScanTasks(
        NAMESPACE,
        TABLE_NAME,
        null,
        null,
        ResidualFilterMode.IGNORE
    );

    Assert.assertTrue("V2 table with deletes should have hasDeleteFiles=true", result.hasDeleteFiles());
    Assert.assertEquals(1, result.getFileScanTasks().size());
    Assert.assertFalse("Should have delete files", result.getFileScanTasks().get(0).deletes().isEmpty());
  }

  @Test
  public void testDeleteFileInfoSerialization()
  {
    final DeleteFileInfo posDelete = new DeleteFileInfo(
        "s3://bucket/data/pos-del.parquet",
        DeleteFileInfo.ContentType.POSITION,
        null
    );
    Assert.assertEquals("s3://bucket/data/pos-del.parquet", posDelete.getPath());
    Assert.assertEquals(DeleteFileInfo.ContentType.POSITION, posDelete.getContentType());
    Assert.assertTrue(posDelete.getEqualityFieldIds().isEmpty());

    final DeleteFileInfo eqDelete = new DeleteFileInfo(
        "s3://bucket/data/eq-del.parquet",
        DeleteFileInfo.ContentType.EQUALITY,
        ImmutableList.of(1, 2)
    );
    Assert.assertEquals(DeleteFileInfo.ContentType.EQUALITY, eqDelete.getContentType());
    Assert.assertEquals(ImmutableList.of(1, 2), eqDelete.getEqualityFieldIds());
  }

  @Test
  public void testIcebergFileTaskInputSourceCreation() throws IOException
  {
    createTableWithEqualityDelete();

    final IcebergCatalog.FileScanResult result = testCatalog.extractFileScanTasks(
        NAMESPACE,
        TABLE_NAME,
        null,
        null,
        ResidualFilterMode.IGNORE
    );

    final String schemaJson = org.apache.iceberg.SchemaParser.toJson(result.getTable().schema());
    Assert.assertNotNull(schemaJson);
    Assert.assertFalse(schemaJson.isEmpty());

    // Verify schema round-trip
    final Schema roundTripped = org.apache.iceberg.SchemaParser.fromJson(schemaJson);
    Assert.assertEquals(tableSchema.columns().size(), roundTripped.columns().size());
  }

  @Test
  public void testEmptyTableReturnsNoRows() throws IOException
  {
    // Create table with no data
    final TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), TABLE_NAME);
    testCatalog.retrieveCatalog().createTable(
        tableId,
        tableSchema,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of("format-version", "2")
    );

    final IcebergInputSource inputSource = new IcebergInputSource(
        TABLE_NAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    final InputSourceReader reader = inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder());
    final List<InputRow> rows = readAll(reader);
    Assert.assertEquals(0, rows.size());
  }

  // --- Helper methods ---

  private List<InputRow> readAll(final InputSourceReader reader) throws IOException
  {
    final List<InputRow> rows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }
    }
    return rows;
  }

  private Table createBaseTable()
  {
    final TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), TABLE_NAME);
    return testCatalog.retrieveCatalog().createTable(
        tableId,
        tableSchema,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of("format-version", "2")
    );
  }

  private DataFile writeDataFile(final Table table) throws IOException
  {
    final GenericRecord r1 = GenericRecord.create(tableSchema);
    r1.setField("order_id", 1);
    r1.setField("product", "Widget");
    r1.setField("amount", 10.0);

    final GenericRecord r2 = GenericRecord.create(tableSchema);
    r2.setField("order_id", 2);
    r2.setField("product", "Gadget");
    r2.setField("amount", 20.0);

    final GenericRecord r3 = GenericRecord.create(tableSchema);
    r3.setField("order_id", 3);
    r3.setField("product", "Doohickey");
    r3.setField("amount", 30.0);

    final List<GenericRecord> records = ImmutableList.of(r1, r2, r3);

    final String filepath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    final OutputFile outputFile = table.io().newOutputFile(filepath);

    final DataWriter<GenericRecord> writer = Parquet.writeData(outputFile)
                                                    .schema(tableSchema)
                                                    .createWriterFunc(GenericParquetWriter::create)
                                                    .overwrite()
                                                    .withSpec(PartitionSpec.unpartitioned())
                                                    .build();
    try {
      for (final GenericRecord rec : records) {
        writer.write(rec);
      }
    }
    finally {
      writer.close();
    }
    return writer.toDataFile();
  }

  private void createTableWithoutDeletes() throws IOException
  {
    final Table table = createBaseTable();
    final DataFile dataFile = writeDataFile(table);
    table.newAppend().appendFile(dataFile).commit();
  }

  private void createTableWithEqualityDelete() throws IOException
  {
    final Table table = createBaseTable();
    final DataFile dataFile = writeDataFile(table);
    table.newAppend().appendFile(dataFile).commit();

    final Schema deleteSchema = new Schema(
        Types.NestedField.required(1, "order_id", Types.IntegerType.get())
    );
    final String deletePath = table.location() + "/data/" + UUID.randomUUID() + "-eq-delete.parquet";
    final OutputFile deleteOutputFile = table.io().newOutputFile(deletePath);

    final EqualityDeleteWriter<GenericRecord> eqDeleteWriter = Parquet.writeDeletes(deleteOutputFile)
                                                                     .forTable(table)
                                                                     .rowSchema(deleteSchema)
                                                                     .createWriterFunc(GenericParquetWriter::create)
                                                                     .overwrite()
                                                                     .equalityFieldIds(1)
                                                                     .buildEqualityWriter();
    try {
      final GenericRecord deleteRecord = GenericRecord.create(deleteSchema);
      deleteRecord.setField("order_id", 2);
      eqDeleteWriter.write(deleteRecord);
    }
    finally {
      eqDeleteWriter.close();
    }

    final DeleteFile eqDeleteFile = eqDeleteWriter.toDeleteFile();
    table.newRowDelta().addDeletes(eqDeleteFile).commit();
  }

  private void createTableWithPositionalDelete() throws IOException
  {
    final Table table = createBaseTable();
    final DataFile dataFile = writeDataFile(table);
    table.newAppend().appendFile(dataFile).commit();

    final String deletePath = table.location() + "/data/" + UUID.randomUUID() + "-pos-delete.parquet";
    final OutputFile deleteOutputFile = table.io().newOutputFile(deletePath);

    final PositionDeleteWriter<GenericRecord> posDeleteWriter = Parquet.writeDeletes(deleteOutputFile)
                                                                      .forTable(table)
                                                                      .createWriterFunc(GenericParquetWriter::create)
                                                                      .rowSchema(tableSchema)
                                                                      .overwrite()
                                                                      .buildPositionWriter();
    try {
      final PositionDelete<GenericRecord> posDelete = PositionDelete.create();
      final GenericRecord deleteRow = GenericRecord.create(tableSchema);
      deleteRow.setField("order_id", 2);
      deleteRow.setField("product", "Gadget");
      deleteRow.setField("amount", 20.0);
      posDelete.set(dataFile.location(), 1L, deleteRow);
      posDeleteWriter.write(posDelete);
    }
    finally {
      posDeleteWriter.close();
    }

    final DeleteFile posDeleteFile = posDeleteWriter.toDeleteFile();
    table.newRowDelta().addDeletes(posDeleteFile).commit();
  }

  @Test
  public void testPositionalDeleteFileScopedToMatchingDataFile() throws IOException
  {
    final Table table = createBaseTable();
    final DataFile dataFile1 = writeDataFile(table);
    final DataFile dataFile2 = writeDataFile(table);
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    final String deletePath = table.location() + "/data/" + UUID.randomUUID() + "-pos-delete.parquet";
    final OutputFile deleteOutputFile = table.io().newOutputFile(deletePath);

    final PositionDeleteWriter<GenericRecord> posDeleteWriter = Parquet.writeDeletes(deleteOutputFile)
                                                                      .forTable(table)
                                                                      .createWriterFunc(GenericParquetWriter::create)
                                                                      .rowSchema(tableSchema)
                                                                      .overwrite()
                                                                      .buildPositionWriter();
    try {
      final PositionDelete<GenericRecord> d1 = PositionDelete.create();
      final GenericRecord row1 = GenericRecord.create(tableSchema);
      row1.setField("order_id", 2);
      row1.setField("product", "Gadget");
      row1.setField("amount", 20.0);
      d1.set(dataFile1.location(), 1L, row1);
      posDeleteWriter.write(d1);

      final PositionDelete<GenericRecord> d2 = PositionDelete.create();
      final GenericRecord row2 = GenericRecord.create(tableSchema);
      row2.setField("order_id", 3);
      row2.setField("product", "Doohickey");
      row2.setField("amount", 30.0);
      d2.set(dataFile2.location(), 2L, row2);
      posDeleteWriter.write(d2);
    }
    finally {
      posDeleteWriter.close();
    }

    table.newRowDelta().addDeletes(posDeleteWriter.toDeleteFile()).commit();

    final IcebergInputSource inputSource = new IcebergInputSource(
        TABLE_NAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    final InputSourceReader reader = inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder());
    final List<InputRow> rows = readAll(reader);

    Assert.assertEquals(4, rows.size());

    final List<String> orderIds = new ArrayList<>();
    for (final InputRow row : rows) {
      orderIds.add(row.getDimension("order_id").get(0));
    }

    int countOne = 0;
    int countTwo = 0;
    int countThree = 0;
    for (final String id : orderIds) {
      if ("1".equals(id)) {
        countOne++;
      } else if ("2".equals(id)) {
        countTwo++;
      } else if ("3".equals(id)) {
        countThree++;
      }
    }
    Assert.assertEquals("order_id=1 appears in both data files, neither deleted", 2, countOne);
    Assert.assertEquals("order_id=2 deleted in dataFile1 only", 1, countTwo);
    Assert.assertEquals("order_id=3 deleted in dataFile2 only", 1, countThree);
  }

  @Test
  public void testEqualityDeleteWithNullValueMatchesNullRows() throws IOException
  {
    final Schema schemaWithOptional = new Schema(
        Types.NestedField.required(1, "order_id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "product", Types.StringType.get())
    );

    final TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), TABLE_NAME);
    final Table table = testCatalog.retrieveCatalog().createTable(
        tableId,
        schemaWithOptional,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of("format-version", "2")
    );

    final GenericRecord r1 = GenericRecord.create(schemaWithOptional);
    r1.setField("order_id", 1);
    r1.setField("product", "Widget");

    final GenericRecord r2 = GenericRecord.create(schemaWithOptional);
    r2.setField("order_id", 2);
    r2.setField("product", null);

    final GenericRecord r3 = GenericRecord.create(schemaWithOptional);
    r3.setField("order_id", 3);
    r3.setField("product", "Gadget");

    final String dataPath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    final OutputFile dataOut = table.io().newOutputFile(dataPath);
    final DataWriter<GenericRecord> dataWriter = Parquet.writeData(dataOut)
                                                       .schema(schemaWithOptional)
                                                       .createWriterFunc(GenericParquetWriter::create)
                                                       .overwrite()
                                                       .withSpec(PartitionSpec.unpartitioned())
                                                       .build();
    try {
      dataWriter.write(r1);
      dataWriter.write(r2);
      dataWriter.write(r3);
    }
    finally {
      dataWriter.close();
    }
    table.newAppend().appendFile(dataWriter.toDataFile()).commit();

    final Schema deleteSchema = new Schema(Types.NestedField.optional(2, "product", Types.StringType.get()));
    final String deletePath = table.location() + "/data/" + UUID.randomUUID() + "-eq-delete.parquet";
    final OutputFile deleteOut = table.io().newOutputFile(deletePath);
    final EqualityDeleteWriter<GenericRecord> eqWriter = Parquet.writeDeletes(deleteOut)
                                                                .forTable(table)
                                                                .rowSchema(deleteSchema)
                                                                .createWriterFunc(GenericParquetWriter::create)
                                                                .overwrite()
                                                                .equalityFieldIds(2)
                                                                .buildEqualityWriter();
    try {
      final GenericRecord deleteRecord = GenericRecord.create(deleteSchema);
      deleteRecord.setField("product", null);
      eqWriter.write(deleteRecord);
    }
    finally {
      eqWriter.close();
    }
    table.newRowDelta().addDeletes(eqWriter.toDeleteFile()).commit();

    final InputRowSchema schemaForRead = new InputRowSchema(
        new TimestampSpec("order_id", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("order_id", "product"))),
        ColumnsFilter.all(),
        ImmutableSet.of()
    );

    final IcebergInputSource inputSource = new IcebergInputSource(
        TABLE_NAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    final InputSourceReader reader = inputSource.reader(schemaForRead, null, temporaryFolder.newFolder());
    final List<InputRow> rows = readAll(reader);

    Assert.assertEquals("Row with product=null should be deleted", 2, rows.size());
    for (final InputRow row : rows) {
      final String orderId = row.getDimension("order_id").get(0);
      Assert.assertNotEquals("order_id=2 (product=null) must be deleted", "2", orderId);
    }
  }

  @Test
  public void testPartitionedTableEqualityDeleteScopedToPartition() throws IOException
  {
    final PartitionSpec spec = PartitionSpec.builderFor(tableSchema).identity("product").build();

    final TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), TABLE_NAME);
    final Table table = testCatalog.retrieveCatalog().createTable(
        tableId,
        tableSchema,
        spec,
        ImmutableMap.of("format-version", "2")
    );

    final DataFile widgetFile = writePartitionedDataFile(table, spec, "Widget", 1, 10.0);
    final DataFile gadgetFile = writePartitionedDataFile(table, spec, "Gadget", 2, 20.0);
    table.newAppend().appendFile(widgetFile).appendFile(gadgetFile).commit();

    final Schema deleteSchema = new Schema(
        Types.NestedField.required(1, "order_id", Types.IntegerType.get())
    );
    final org.apache.iceberg.PartitionKey widgetKey = new org.apache.iceberg.PartitionKey(spec, tableSchema);
    final GenericRecord widgetRowForKey = GenericRecord.create(tableSchema);
    widgetRowForKey.setField("order_id", 1);
    widgetRowForKey.setField("product", "Widget");
    widgetRowForKey.setField("amount", 10.0);
    widgetKey.partition(widgetRowForKey);

    final String deletePath = table.location() + "/data/" + UUID.randomUUID() + "-eq-delete.parquet";
    final OutputFile deleteOut = table.io().newOutputFile(deletePath);
    final EqualityDeleteWriter<GenericRecord> eqWriter = Parquet.writeDeletes(deleteOut)
                                                                .forTable(table)
                                                                .rowSchema(deleteSchema)
                                                                .createWriterFunc(GenericParquetWriter::create)
                                                                .overwrite()
                                                                .withPartition(widgetKey)
                                                                .equalityFieldIds(1)
                                                                .buildEqualityWriter();
    try {
      final GenericRecord deleteRecord = GenericRecord.create(deleteSchema);
      deleteRecord.setField("order_id", 1);
      eqWriter.write(deleteRecord);
    }
    finally {
      eqWriter.close();
    }
    table.newRowDelta().addDeletes(eqWriter.toDeleteFile()).commit();

    final IcebergInputSource inputSource = new IcebergInputSource(
        TABLE_NAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );

    final InputSourceReader reader = inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder());
    final List<InputRow> rows = readAll(reader);

    Assert.assertEquals("Only Widget partition row deleted; Gadget partition unaffected", 1, rows.size());
    Assert.assertEquals("2", rows.get(0).getDimension("order_id").get(0));
    Assert.assertEquals("Gadget", rows.get(0).getDimension("product").get(0));
  }

  @Test
  public void testV2CreateSplitsReturnsOneSplitPerDataFile() throws IOException
  {
    final Table table = createBaseTable();
    final DataFile dataFile1 = writeDataFile(table);
    final DataFile dataFile2 = writeDataFile(table);
    final DataFile dataFile3 = writeDataFile(table);
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).appendFile(dataFile3).commit();

    writePositionalDeleteFor(table, dataFile1, 0L);

    final IcebergInputSource inputSource = newIcebergInputSource();
    final List<InputSplit<List<String>>> splits = inputSource.createSplits(null, null).collect(Collectors.toList());

    Assert.assertEquals("expected one split per data file", 3, splits.size());
    final List<String> splitPaths = splits.stream().map(s -> s.get().get(0)).collect(Collectors.toList());
    Assert.assertTrue(splitPaths.contains(dataFile1.location()));
    Assert.assertTrue(splitPaths.contains(dataFile2.location()));
    Assert.assertTrue(splitPaths.contains(dataFile3.location()));
  }

  @Test
  public void testV2EstimateNumSplitsMatchesDataFileCount() throws IOException
  {
    final Table table = createBaseTable();
    final DataFile dataFile1 = writeDataFile(table);
    final DataFile dataFile2 = writeDataFile(table);
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();
    writePositionalDeleteFor(table, dataFile1, 0L);

    final IcebergInputSource inputSource = newIcebergInputSource();
    Assert.assertEquals(2, inputSource.estimateNumSplits(null, null));
  }

  @Test
  public void testV2WithSplitReturnsTaskInputSourceForMatchingDataFile() throws IOException
  {
    final Table table = createBaseTable();
    final DataFile dataFile1 = writeDataFile(table);
    final DataFile dataFile2 = writeDataFile(table);
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();
    writePositionalDeleteFor(table, dataFile1, 1L);

    final IcebergInputSource inputSource = newIcebergInputSource();

    final InputSplit<List<String>> splitForFile1 = new InputSplit<>(Collections.singletonList(dataFile1.location()));
    final InputSource picked = inputSource.withSplit(splitForFile1);

    Assert.assertTrue(picked instanceof IcebergFileTaskInputSource);
    Assert.assertEquals(dataFile1.location(), ((IcebergFileTaskInputSource) picked).getDataFilePath());

    final List<InputRow> rows = readAll(picked.reader(inputRowSchema, null, temporaryFolder.newFolder()));
    Assert.assertEquals("dataFile1 has 3 rows minus 1 positional delete", 2, rows.size());
  }

  @Test
  public void testV2WithSplitThrowsForUnknownDataFile() throws IOException
  {
    final Table table = createBaseTable();
    final DataFile dataFile = writeDataFile(table);
    table.newAppend().appendFile(dataFile).commit();
    writePositionalDeleteFor(table, dataFile, 0L);

    final IcebergInputSource inputSource = newIcebergInputSource();

    final InputSplit<List<String>> bogus = new InputSplit<>(Collections.singletonList("/does/not/exist.parquet"));
    Assert.assertThrows(ISE.class, () -> inputSource.withSplit(bogus));
  }

  @Test
  public void testV2GetSplitHintSpecOrDefaultIsFilePerSplit() throws IOException
  {
    final Table table = createBaseTable();
    final DataFile dataFile = writeDataFile(table);
    table.newAppend().appendFile(dataFile).commit();
    writePositionalDeleteFor(table, dataFile, 0L);

    final IcebergInputSource inputSource = newIcebergInputSource();

    Assert.assertSame(FilePerSplitHintSpec.INSTANCE, inputSource.getSplitHintSpecOrDefault(null));
  }

  private IcebergInputSource newIcebergInputSource()
  {
    return new IcebergInputSource(
        TABLE_NAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null
    );
  }

  private void writePositionalDeleteFor(final Table table, final DataFile dataFile, final long position)
      throws IOException
  {
    final String deletePath = table.location() + "/data/" + UUID.randomUUID() + "-pos-delete.parquet";
    final OutputFile deleteOut = table.io().newOutputFile(deletePath);
    final PositionDeleteWriter<GenericRecord> writer = Parquet.writeDeletes(deleteOut)
                                                              .forTable(table)
                                                              .createWriterFunc(GenericParquetWriter::create)
                                                              .rowSchema(tableSchema)
                                                              .overwrite()
                                                              .buildPositionWriter();
    try {
      final PositionDelete<GenericRecord> d = PositionDelete.create();
      final GenericRecord row = GenericRecord.create(tableSchema);
      row.setField("order_id", 0);
      row.setField("product", "any");
      row.setField("amount", 0.0);
      d.set(dataFile.location(), position, row);
      writer.write(d);
    }
    finally {
      writer.close();
    }
    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
  }

  private DataFile writePartitionedDataFile(
      final Table table,
      final PartitionSpec spec,
      final String product,
      final int orderId,
      final double amount
  ) throws IOException
  {
    final GenericRecord rec = GenericRecord.create(tableSchema);
    rec.setField("order_id", orderId);
    rec.setField("product", product);
    rec.setField("amount", amount);

    final org.apache.iceberg.PartitionKey key = new org.apache.iceberg.PartitionKey(spec, tableSchema);
    key.partition(rec);

    final String filepath = table.location() + "/data/" + product + "/" + UUID.randomUUID() + ".parquet";
    final OutputFile outputFile = table.io().newOutputFile(filepath);

    final DataWriter<GenericRecord> writer = Parquet.writeData(outputFile)
                                                    .schema(tableSchema)
                                                    .createWriterFunc(GenericParquetWriter::create)
                                                    .overwrite()
                                                    .withSpec(spec)
                                                    .withPartition(key)
                                                    .build();
    try {
      writer.write(rec);
    }
    finally {
      writer.close();
    }
    return writer.toDataFile();
  }
}
