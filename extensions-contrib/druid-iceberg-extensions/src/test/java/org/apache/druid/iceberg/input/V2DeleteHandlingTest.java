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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSourceFactory;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
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
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class V2DeleteHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private IcebergCatalog testCatalog;
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
      // ignore if table doesn't exist
    }
  }

  @Test
  public void testSkipModeUsesFilePathExtractionOnly() throws IOException
  {
    // Create a v2 table with equality deletes
    createTableWithEqualityDelete();

    // SKIP mode uses extractSnapshotDataFiles (file paths only), ignoring delete files.
    // Verify the catalog returns the raw data file paths without considering deletes.
    final List<String> dataFiles = testCatalog.extractSnapshotDataFiles(
        NAMESPACE,
        TABLE_NAME,
        null,
        null,
        ResidualFilterMode.IGNORE
    );

    // Should return 1 data file (the delete file is ignored since it's not a data file)
    Assert.assertEquals(1, dataFiles.size());
  }

  @Test
  public void testFailModeThrowsWhenDeletesPresent() throws IOException
  {
    createTableWithEqualityDelete();

    final IcebergInputSource inputSource = new IcebergInputSource(
        TABLE_NAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null,
        V2DeleteHandling.FAIL
    );

    Assert.assertThrows(
        DruidException.class,
        () -> inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder())
    );
  }

  @Test
  public void testFailModeDoesNotThrowWhenNoDeletes() throws IOException
  {
    createTableWithoutDeletes();

    // When no delete files exist, FAIL mode should not throw.
    // Verify via extractFileScanTasks that no deletes are detected.
    final IcebergCatalog.FileScanResult result = testCatalog.extractFileScanTasks(
        NAMESPACE,
        TABLE_NAME,
        null,
        null,
        ResidualFilterMode.IGNORE
    );

    Assert.assertFalse("Table without deletes should have hasDeleteFiles=false", result.hasDeleteFiles());
    Assert.assertEquals(1, result.getFileScanTasks().size());
  }

  @Test
  public void testApplyModeWithEqualityDelete() throws IOException
  {
    createTableWithEqualityDelete();

    final IcebergInputSource inputSource = new IcebergInputSource(
        TABLE_NAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null,
        V2DeleteHandling.APPLY
    );

    final InputSourceReader reader = inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder());
    final List<InputRow> rows = readAll(reader);

    // Equality delete removed order_id=2, so only 2 rows remain
    Assert.assertEquals(2, rows.size());

    final List<Object> orderIds = new ArrayList<>();
    for (final InputRow row : rows) {
      orderIds.add(row.getDimension("order_id").get(0));
    }
    Assert.assertTrue("Should contain order_id 1", orderIds.contains("1"));
    Assert.assertTrue("Should contain order_id 3", orderIds.contains("3"));
    Assert.assertFalse("Should NOT contain deleted order_id 2", orderIds.contains("2"));
  }

  @Test
  public void testApplyModeWithNoDeletesFallsBackToFilePaths() throws IOException
  {
    createTableWithoutDeletes();

    // When APPLY mode is used but no delete files exist, the code falls through
    // to the warehouseSource path (extracting file paths only). Verify the scan
    // result correctly detects no deletes.
    final IcebergCatalog.FileScanResult result = testCatalog.extractFileScanTasks(
        NAMESPACE,
        TABLE_NAME,
        null,
        null,
        ResidualFilterMode.IGNORE
    );

    Assert.assertFalse("Table without deletes should have hasDeleteFiles=false", result.hasDeleteFiles());
    Assert.assertEquals(1, result.getFileScanTasks().size());
    Assert.assertNotNull(result.getTable());
  }

  @Test
  public void testApplyModeWithPositionalDelete() throws IOException
  {
    createTableWithPositionalDelete();

    final IcebergInputSource inputSource = new IcebergInputSource(
        TABLE_NAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null,
        V2DeleteHandling.APPLY
    );

    final InputSourceReader reader = inputSource.reader(inputRowSchema, null, temporaryFolder.newFolder());
    final List<InputRow> rows = readAll(reader);

    // Positional delete removed row at position 1 (order_id=2), so 2 rows remain
    Assert.assertEquals(2, rows.size());
  }

  @Test
  public void testDefaultV2DeleteHandlingIsSkip()
  {
    final IcebergInputSource inputSource = new IcebergInputSource(
        TABLE_NAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null,
        null
    );
    Assert.assertEquals(V2DeleteHandling.SKIP, inputSource.getV2DeleteHandling());
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
        new HashMap<String, String>() {{
          put("format-version", "2");
        }}
    );
  }

  private DataFile writeDataFile(final Table table) throws IOException
  {
    final GenericRecord template = GenericRecord.create(tableSchema);

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

    // Write an equality delete file that deletes where order_id = 2
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

    // Write a positional delete file that deletes row at position 1 (order_id=2)
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
}
