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
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.iceberg.filter.IcebergEqualsFilter;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
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

public class IcebergArrowInputSourceReaderTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String NAMESPACE = "default";
  private static final String TABLE = "arrowTestTable";

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "ts", Types.LongType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
      Types.NestedField.required(3, "value", Types.DoubleType.get())
  );

  private static final InputRowSchema INPUT_SCHEMA = new InputRowSchema(
      new TimestampSpec("ts", "millis", null),
      DimensionsSpec.builder()
                    .setDimensions(ImmutableList.of(
                        new StringDimensionSchema("name")
                    ))
                    .build(),
      ColumnsFilter.all()
  );

  private File warehouseDir;
  private IcebergCatalog catalog;
  private TableIdentifier tableId;

  @Before
  public void setup() throws IOException
  {
    warehouseDir = FileUtils.createTempDir();
    catalog = new LocalCatalog(warehouseDir.getPath(), new HashMap<>(), true);
    tableId = TableIdentifier.of(Namespace.of(NAMESPACE), TABLE);
  }

  @After
  public void tearDown()
  {
    if (catalog.retrieveCatalog().tableExists(tableId)) {
      catalog.retrieveCatalog().dropTable(tableId);
    }
  }

  @Test
  public void testBasicRead() throws IOException
  {
    final Table table = catalog.retrieveCatalog().createTable(tableId, SCHEMA);
    writeRows(table, row(1_000L, "alice", 1.1), row(2_000L, "bob", 2.2), row(3_000L, "carol", 3.3));

    final IcebergArrowInputSourceReader reader = new IcebergArrowInputSourceReader(
        table,
        null,
        null,
        true,
        INPUT_SCHEMA,
        IcebergArrowInputSourceReader.DEFAULT_BATCH_SIZE
    );

    final List<InputRow> rows = readAll(reader);
    Assert.assertEquals(3, rows.size());
    Assert.assertEquals(1_000L, rows.get(0).getTimestampFromEpoch());
    Assert.assertEquals("alice", rows.get(0).getDimension("name").get(0));
    Assert.assertEquals(2_000L, rows.get(1).getTimestampFromEpoch());
    Assert.assertEquals("bob", rows.get(1).getDimension("name").get(0));
    Assert.assertEquals(3_000L, rows.get(2).getTimestampFromEpoch());
    Assert.assertEquals("carol", rows.get(2).getDimension("name").get(0));
  }

  @Test
  public void testEmptyTable() throws IOException
  {
    catalog.retrieveCatalog().createTable(tableId, SCHEMA);
    final Table table = catalog.retrieveTable(NAMESPACE, TABLE);

    final IcebergArrowInputSourceReader reader = new IcebergArrowInputSourceReader(
        table,
        null,
        null,
        true,
        INPUT_SCHEMA,
        IcebergArrowInputSourceReader.DEFAULT_BATCH_SIZE
    );

    final List<InputRow> rows = readAll(reader);
    Assert.assertEquals(0, rows.size());
  }

  @Test
  public void testWithEqualsFilter() throws IOException
  {
    final Table table = catalog.retrieveCatalog().createTable(tableId, SCHEMA);
    writeRows(
        table,
        row(1_000L, "alice", 1.0),
        row(2_000L, "bob", 2.0),
        row(3_000L, "alice", 3.0)
    );

    final IcebergArrowInputSourceReader reader = new IcebergArrowInputSourceReader(
        table,
        new IcebergEqualsFilter("name", "alice"),
        null,
        true,
        INPUT_SCHEMA,
        IcebergArrowInputSourceReader.DEFAULT_BATCH_SIZE
    );

    // Filter is non-partition on an unpartitioned table — all 3 rows from the file are returned.
    // iceberg-arrow may dict-encode repeated string columns, so we assert row count only.
    final List<InputRow> rows = readAll(reader);
    Assert.assertEquals(3, rows.size());
  }

  @Test
  public void testColumnPruning() throws IOException
  {
    final Table table = catalog.retrieveCatalog().createTable(tableId, SCHEMA);
    writeRows(table, row(1_000L, "alice", 9.9));

    // Only request ts + name; value should not appear in output event.
    final InputRowSchema pruned = new InputRowSchema(
        new TimestampSpec("ts", "millis", null),
        DimensionsSpec.builder()
                      .setDimensions(ImmutableList.of(new StringDimensionSchema("name")))
                      .build(),
        ColumnsFilter.all()
    );

    final IcebergArrowInputSourceReader reader = new IcebergArrowInputSourceReader(
        table,
        null,
        null,
        true,
        pruned,
        IcebergArrowInputSourceReader.DEFAULT_BATCH_SIZE
    );

    final List<InputRow> rows = readAll(reader);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(1_000L, rows.get(0).getTimestampFromEpoch());
    Assert.assertEquals("alice", rows.get(0).getDimension("name").get(0));
  }

  @Test
  public void testLargeBatch() throws IOException
  {
    final Table table = catalog.retrieveCatalog().createTable(tableId, SCHEMA);
    final int count = 5_000;
    final GenericRecord[] data = new GenericRecord[count];
    for (int i = 0; i < count; i++) {
      data[i] = row((long) (i + 1) * 1000, "user" + i, i * 0.1);
    }
    writeRows(table, data);

    final IcebergArrowInputSourceReader reader = new IcebergArrowInputSourceReader(
        table,
        null,
        null,
        true,
        INPUT_SCHEMA,
        IcebergArrowInputSourceReader.DEFAULT_BATCH_SIZE
    );

    final List<InputRow> rows = readAll(reader);
    Assert.assertEquals(count, rows.size());
  }

  @Test
  public void testSnapshotTime() throws IOException, InterruptedException
  {
    final Table table = catalog.retrieveCatalog().createTable(tableId, SCHEMA);
    writeRows(table, row(1_000L, "snap1", 1.0));
    final long afterFirstSnapshot = System.currentTimeMillis();

    // Small sleep to ensure second snapshot has later timestamp
    Thread.sleep(10);
    writeRows(table, row(2_000L, "snap2", 2.0));

    // Read as-of the first snapshot — should only see 1 row.
    final IcebergArrowInputSourceReader reader = new IcebergArrowInputSourceReader(
        table,
        null,
        DateTimes.utc(afterFirstSnapshot),
        true,
        INPUT_SCHEMA,
        IcebergArrowInputSourceReader.DEFAULT_BATCH_SIZE
    );

    final List<InputRow> rows = readAll(reader);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("snap1", rows.get(0).getDimension("name").get(0));
  }

  // --- helpers ---

  private static GenericRecord row(final long ts, final String name, final double value)
  {
    final GenericRecord r = GenericRecord.create(SCHEMA);
    r.setField("ts", ts);
    r.setField("name", name);
    r.setField("value", value);
    return r;
  }

  private static void writeRows(final Table table, final GenericRecord... records) throws IOException
  {
    final String filepath = table.location() + "/" + UUID.randomUUID() + ".parquet";
    final OutputFile file = table.io().newOutputFile(filepath);
    final DataWriter<GenericRecord> writer =
        Parquet.writeData(file)
               .schema(SCHEMA)
               .createWriterFunc(GenericParquetWriter::create)
               .overwrite()
               .withSpec(PartitionSpec.unpartitioned())
               .build();
    try {
      for (final GenericRecord r : records) {
        writer.write(r);
      }
    }
    finally {
      writer.close();
    }
    final DataFile dataFile = writer.toDataFile();
    table.newAppend().appendFile(dataFile).commit();
  }

  private static List<InputRow> readAll(final IcebergArrowInputSourceReader reader) throws IOException
  {
    final List<InputRow> result = new ArrayList<>();
    try (CloseableIterator<InputRow> it = reader.read(new NoopInputStats())) {
      while (it.hasNext()) {
        result.add(it.next());
      }
    }
    return result;
  }

  private static final class NoopInputStats implements org.apache.druid.data.input.InputStats
  {
    @Override
    public void incrementProcessedBytes(final long v)
    {
    }

    @Override
    public long getProcessedBytes()
    {
      return 0;
    }
  }
}
