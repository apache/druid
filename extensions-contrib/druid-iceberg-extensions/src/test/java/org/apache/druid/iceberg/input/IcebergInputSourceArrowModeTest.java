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
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSourceFactory;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.iceberg.filter.IcebergEqualsFilter;
import org.apache.druid.iceberg.filter.IcebergFilter;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
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
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Covers {@link IcebergInputSource} with {@code useArrowReader} enabled, where the Arrow delegate reads the
 * table scan directly instead of going through a warehouse input source.
 */
public class IcebergInputSourceArrowModeTest
{
  private IcebergCatalog testCatalog;
  private TableIdentifier tableIdentifier;
  private File warehouseDir;

  private final Schema tableSchema = new Schema(
      Types.NestedField.required(1, "id", Types.StringType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get())
  );
  private final Map<String, Object> tableData = ImmutableMap.of("id", "123988", "name", "Foo");

  private static final String NAMESPACE = "default";
  private static final String TABLENAME = "foosTable";

  @BeforeEach
  public void setup() throws IOException
  {
    warehouseDir = FileUtils.createTempDir();
    testCatalog = new LocalCatalog(warehouseDir.getPath(), new HashMap<>(), true);
    tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), TABLENAME);
    createAndLoadTable(tableIdentifier);
  }

  @AfterEach
  public void tearDown()
  {
    dropTableFromCatalog(tableIdentifier);
  }

  @Test
  public void testReadWithNullInputStatsDoesNotNpe() throws IOException
  {
    final IcebergInputSource src = arrowSource(null, null, null);
    final InputRowSchema schemaWithMissingTs = new InputRowSchema(
        new TimestampSpec(null, null, DateTimes.utc(0L)),
        DimensionsSpec.builder().build(),
        ColumnsFilter.all()
    );
    final InputSourceReader reader = src.reader(schemaWithMissingTs, null, FileUtils.createTempDir());
    try (CloseableIterator<InputRow> it = reader.read()) {
      while (it.hasNext()) {
        Assertions.assertNotNull(it.next());
      }
    }
  }

  @Test
  public void testIsNotSplittable()
  {
    final IcebergInputSource src = arrowSource(null, null, null);
    Assertions.assertFalse(src.isSplittable());
    Assertions.assertFalse(src.needsFormat());
  }

  @Test
  public void testSingleSplitReturnsOwningSourceSoRowsAreNotDropped() throws IOException
  {
    final IcebergInputSource src = arrowSource(null, null, null);
    Assertions.assertEquals(1, src.estimateNumSplits(null, null));
    final InputSource split = src.createSplits(null, null)
                                 .map(src::withSplit)
                                 .findFirst()
                                 .orElseThrow(() -> new IllegalStateException("expected one split"));
    Assertions.assertSame(src, split);
  }

  @Test
  public void testResidualFilterModeFail()
  {
    final IcebergInputSource src = arrowSource(
        new IcebergEqualsFilter("id", "123988"),
        null,
        ResidualFilterMode.FAIL
    );
    final InputRowSchema inputRowSchema = new InputRowSchema(
        new TimestampSpec("timestamp", "millis", null),
        DimensionsSpec.builder().build(),
        ColumnsFilter.all()
    );
    final DruidException ex = Assertions.assertThrows(
        DruidException.class,
        () -> {
          final InputSourceReader reader = src.reader(inputRowSchema, null, FileUtils.createTempDir());
          reader.read().close();
        }
    );
    Assertions.assertTrue(
        ex.getMessage().contains("residual"),
        "Expected residual error: " + ex.getMessage()
    );
  }

  @Test
  public void testResidualFilterModeFailUsesSnapshotTime() throws Exception
  {
    final String filterId = (String) tableData.get("id");
    dropTableFromCatalog(tableIdentifier);
    final PartitionSpec partitionSpec = PartitionSpec.builderFor(tableSchema)
                                                     .identity("id")
                                                     .build();
    final Table table = testCatalog.retrieveCatalog().createTable(tableIdentifier, tableSchema, partitionSpec);
    appendRow(table, partitionSpec, tableData);

    final long afterPartitionedSnapshot = System.currentTimeMillis();
    Thread.sleep(10);

    table.updateSpec().removeField("id").commit();
    appendRow(table, table.spec(), ImmutableMap.of("id", filterId, "name", "Bar"));

    final IcebergInputSource src = arrowSource(
        new IcebergEqualsFilter("id", filterId),
        DateTimes.utc(afterPartitionedSnapshot),
        ResidualFilterMode.FAIL
    );
    final InputRowSchema inputRowSchema = new InputRowSchema(
        new TimestampSpec(null, null, DateTimes.utc(0L)),
        DimensionsSpec.builder().build(),
        ColumnsFilter.all()
    );

    final InputSourceReader reader = src.reader(inputRowSchema, null, FileUtils.createTempDir());
    reader.read().close();
  }

  @Test
  public void testWarehouseSourceNotRequiredInArrowMode()
  {
    Assertions.assertFalse(arrowSource(null, null, null).isSplittable());
  }

  @Test
  public void testWarehouseSourceRequiredWhenArrowDisabled()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new IcebergInputSource(
            TABLENAME,
            NAMESPACE,
            null,
            testCatalog,
            null,
            null,
            null,
            false,
            null
        )
    );
  }

  @Test
  public void testArrowPropertiesSurviveSerde() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(
        new NamedType(LocalCatalog.class, LocalCatalog.TYPE_KEY),
        new NamedType(IcebergInputSource.class, IcebergInputSource.TYPE_KEY)
    );
    final IcebergInputSource src = arrowSource(null, null, null);
    final IcebergInputSource roundTripped = (IcebergInputSource) mapper.readValue(
        mapper.writeValueAsBytes(src),
        InputSource.class
    );
    Assertions.assertTrue(roundTripped.isUseArrowReader());
    Assertions.assertEquals(512, roundTripped.getArrowBatchSize());
    Assertions.assertFalse(roundTripped.needsFormat());
    Assertions.assertFalse(roundTripped.isSplittable());
  }

  @Test
  public void testDefaultsToStandardModeWhenArrowUnset()
  {
    final IcebergInputSource src = new IcebergInputSource(
        TABLENAME,
        NAMESPACE,
        null,
        testCatalog,
        new LocalInputSourceFactory(),
        null,
        null,
        null,
        null
    );
    Assertions.assertFalse(src.isUseArrowReader());
    Assertions.assertTrue(src.needsFormat());
    Assertions.assertTrue(src.isSplittable());
  }

  private IcebergInputSource arrowSource(
      final IcebergFilter icebergFilter,
      final DateTime snapshotTime,
      final ResidualFilterMode residualFilterMode
  )
  {
    return new IcebergInputSource(
        TABLENAME,
        NAMESPACE,
        icebergFilter,
        testCatalog,
        null,
        snapshotTime,
        residualFilterMode,
        true,
        512
    );
  }

  private void createAndLoadTable(TableIdentifier id) throws IOException
  {
    final Table table = testCatalog.retrieveCatalog().createTable(id, tableSchema, PartitionSpec.unpartitioned());
    appendRow(table, PartitionSpec.unpartitioned(), tableData);
  }

  private void appendRow(Table table, PartitionSpec partitionSpec, Map<String, Object> rowData) throws IOException
  {
    final String fname = UUID.randomUUID() + ".parquet";
    final File dataFile = new File(warehouseDir.getAbsolutePath() + "/" + fname);
    Assertions.assertTrue(dataFile.createNewFile());
    final OutputFile out = Files.localOutput(dataFile);
    final GenericRecord row = GenericRecord.create(tableSchema);
    row.setField("id", rowData.get("id"));
    row.setField("name", rowData.get("name"));
    final DataWriter<Record> writer;
    if (partitionSpec.isUnpartitioned()) {
      writer = Parquet.writeData(out)
                      .schema(tableSchema)
                      .createWriterFunc(GenericParquetWriter::create)
                      .overwrite()
                      .withSpec(partitionSpec)
                      .build();
    } else {
      final PartitionKey partitionKey = new PartitionKey(partitionSpec, tableSchema);
      partitionKey.partition(row);
      writer = Parquet.writeData(out)
                      .schema(tableSchema)
                      .createWriterFunc(GenericParquetWriter::create)
                      .overwrite()
                      .withSpec(partitionSpec)
                      .withPartition(partitionKey)
                      .build();
    }
    try {
      writer.write(row);
    }
    finally {
      writer.close();
    }
    final DataFile df = writer.toDataFile();
    table.newAppend().appendFile(df).commit();
  }

  private void dropTableFromCatalog(TableIdentifier id)
  {
    testCatalog.retrieveCatalog().dropTable(id);
  }
}
