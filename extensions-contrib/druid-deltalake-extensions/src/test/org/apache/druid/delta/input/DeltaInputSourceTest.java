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

package org.apache.druid.delta.input;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;


import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class DeltaInputSourceTest
{

  @Test
  public void testInputSourceInit() throws IOException
  {
//    String deltaTablePath = "/Users/abhishek/Desktop/people-delta-table-oss";
    String deltaTablePath = "/var/folders/5g/jnsnl96j4wlfw404lyj89mxm0000gr/T/my_table9175802766602445708";

    final DeltaInputSource deltaInputSource = new DeltaInputSource(deltaTablePath, null);

    Assert.assertNotNull(deltaInputSource);

    InputRowSchema someSchema = new InputRowSchema(
        new TimestampSpec("no_such_col!!", "auto", DateTimes.of("1970")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("city", "state"))),
        ColumnsFilter.all()
    );

    InputSourceReader inputSourceReader = deltaInputSource.reader(someSchema, null, null);
    Assert.assertNotNull(inputSourceReader);
    CloseableIterator<InputRow> rowReader = inputSourceReader.read();
    Assert.assertNotNull(rowReader);
    int expectedCnt = 2;
    int actualCnt = 0;

    while(rowReader.hasNext()) {
      InputRow row = rowReader.next();
      Assert.assertNotNull(row);
      actualCnt += 1;
      System.out.println("row:" + row);
      Assert.assertNotNull(row.getDimensions());
      Assert.assertNotNull(row.getDimensions());
      if (actualCnt == 1) {
        Assert.assertEquals(Collections.singletonList("Montclair"), row.getDimension("city"));
      } else {
        Assert.assertEquals(Collections.singletonList("Wildwood"), row.getDimension("city"));
      }
    }
    Assert.assertEquals(expectedCnt, actualCnt);
  }

  @Test
  public void testWriteDeltaLake() throws IOException
  {
//    File tmpDir = Files.createTempDirectory("my_table").toFile();
//    File tmpDir = Files.createTempDirectory("my_table").toFile();
//    String tmpDirPath = tmpDir.getAbsolutePath();

    String tmpDirPath = "/Users/abhishek/Desktop/imply/abhishek-agarwal-druid/druid/ut-table";

    try {
      final String engineInfo = "local";

      DeltaLog log = DeltaLog.forTable(new Configuration(), tmpDirPath);

      StructType schema = new StructType()
          .add("foo", new IntegerType())
          .add("bar", new IntegerType())
          .add("zip", new StringType());

      List<String> partitionColumns = Arrays.asList("foo", "bar");

      Metadata metadata = Metadata.builder()
                                  .schema(schema)
                                  .partitionColumns(partitionColumns)
                                  .build();

      Operation op = new Operation(Operation.Name.WRITE);

      for (int i = 0; i < 10; i++) {
        OptimisticTransaction txn = log.startTransaction();
        if (i == 0) {
          txn.updateMetadata(metadata);
        }

        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("foo", Integer.toString(i % 3));
        partitionValues.put("bar", Integer.toString(i % 2));

        long now = System.currentTimeMillis();

        AddFile addFile = AddFile.builder(Integer.toString(i+100), partitionValues, 100L, now, true)
                                 .tags(Collections.singletonMap("someTagKey", "someTagVal"))
                                 .build();

        txn.commit(Collections.singletonList(addFile), op, engineInfo);
        System.out.println(String.format("Committed version %d", i));
      }

      DeltaLog log2 = DeltaLog.forTable(new Configuration(), tmpDirPath);
      Set<Integer> pathVals = log2.update()
                                  .getAllFiles()
                                  .stream()
                                  .map(addFile -> Integer.parseInt(addFile.getPath()))
                                  .collect(Collectors.toSet());
//
//      for (int i = 0; i < 10; i++) {
//        if (!pathVals.contains(i)) throw new RuntimeException();
//        System.out.println(String.format("Read version %d", i));
//      }

    } finally {
      System.out.println("Written to " + tmpDirPath);
//      FileUtils.deleteDirectory(tmpDir);
    }

    // now try to read stuff
    final DeltaInputSource deltaInputSource = new DeltaInputSource(tmpDirPath, null);
    Assert.assertNotNull(deltaInputSource);

    InputRowSchema someSchema = new InputRowSchema(
        new TimestampSpec("no_such_col!!", "auto", DateTimes.of("1970")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo", "bar"))),
        ColumnsFilter.all()
    );

    InputSourceReader inputSourceReader = deltaInputSource.reader(someSchema, null, null);
    Assert.assertNotNull(inputSourceReader);

    CloseableIterator<InputRow> rowReader = inputSourceReader.read();
    Assert.assertNotNull(rowReader);

    while(rowReader.hasNext()) {
      InputRow row = rowReader.next();
      Assert.assertNotNull(row);
      System.out.println("row:" + row);
      Assert.assertNotNull(row.getDimensions());
    }
  }
}