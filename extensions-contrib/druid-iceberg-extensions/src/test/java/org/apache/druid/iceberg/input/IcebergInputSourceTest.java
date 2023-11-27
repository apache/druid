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
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.LocalInputSourceFactory;
import org.apache.druid.iceberg.filter.IcebergEqualsFilter;
import org.apache.druid.java.util.common.DateTimes;
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
    final File warehouseDir = FileUtils.createTempDir();
    testCatalog = new LocalCatalog(warehouseDir.getPath(), new HashMap<>());
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
        DateTimes.nowUtc()
    );
    Stream<InputSplit<List<String>>> splits = inputSource.createSplits(null, new MaxSizeSplitHintSpec(null, null));
    Assert.assertEquals(1, splits.count());
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

  private void dropTableFromCatalog(TableIdentifier tableIdentifier)
  {
    testCatalog.retrieveCatalog().dropTable(tableIdentifier);
  }

}
