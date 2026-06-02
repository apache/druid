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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.iceberg.filter.IcebergEqualsFilter;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class IcebergArrowInputSourceTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

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

  @Before
  public void setup() throws IOException
  {
    warehouseDir = FileUtils.createTempDir();
    testCatalog = new LocalCatalog(warehouseDir.getPath(), new HashMap<>(), true);
    tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), TABLENAME);
    createAndLoadTable(tableIdentifier);
  }

  @After
  public void tearDown()
  {
    dropTableFromCatalog(tableIdentifier);
  }

  @Test
  public void testIsNotSplittable()
  {
    final IcebergArrowInputSource src = new IcebergArrowInputSource(
        TABLENAME,
        NAMESPACE,
        null,
        testCatalog,
        null,
        null,
        1024
    );
    Assert.assertFalse(src.isSplittable());
    Assert.assertFalse(src.needsFormat());
  }

  @Test
  public void testResidualFilterModeFail() throws IOException
  {
    final IcebergArrowInputSource src = new IcebergArrowInputSource(
        TABLENAME,
        NAMESPACE,
        new IcebergEqualsFilter("id", "123988"),
        testCatalog,
        null,
        ResidualFilterMode.FAIL,
        1024
    );
    final InputRowSchema inputRowSchema = new InputRowSchema(
        new TimestampSpec("timestamp", "millis", null),
        DimensionsSpec.builder().build(),
        ColumnsFilter.all()
    );
    final DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> {
          final InputSourceReader reader = src.reader(inputRowSchema, null, FileUtils.createTempDir());
          reader.read().close();
        }
    );
    Assert.assertTrue(
        "Expected residual error: " + ex.getMessage(),
        ex.getMessage().contains("residual")
    );
  }

  private void createAndLoadTable(TableIdentifier id) throws IOException
  {
    final Table table = testCatalog.retrieveCatalog().createTable(id, tableSchema, PartitionSpec.unpartitioned());
    final String fname = UUID.randomUUID() + ".parquet";
    final File datafile = new File(warehouseDir.getAbsolutePath() + "/" + fname);
    Assert.assertTrue(datafile.createNewFile());
    final OutputFile out = Files.localOutput(datafile);
    final DataWriter<Record> writer = Parquet.writeData(out)
                                             .schema(tableSchema)
                                             .createWriterFunc(GenericParquetWriter::create)
                                             .overwrite()
                                             .withSpec(PartitionSpec.unpartitioned())
                                             .build();
    final GenericRecord row = GenericRecord.create(tableSchema);
    row.setField("id", tableData.get("id"));
    row.setField("name", tableData.get("name"));
    writer.write(row);
    writer.close();
    final DataFile df = writer.toDataFile();
    table.newAppend().appendFile(df).commit();
  }

  private void dropTableFromCatalog(TableIdentifier id)
  {
    testCatalog.retrieveCatalog().dropTable(id);
  }
}
