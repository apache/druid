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

package org.apache.druid.data.input.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.data.input.impl.systemfield.SystemFieldDecoratorFactory;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class InputEntityIteratingReaderTest extends InitializedNullHandlingTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void test() throws IOException
  {
    final int numFiles = 5;
    final List<File> files = new ArrayList<>();
    long totalFileSize = 0;
    for (int i = 0; i < numFiles; i++) {
      final File file = temporaryFolder.newFile("test_" + i);
      files.add(file);
      try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("%d,%s,%d\n", 20190101 + i, "name_" + i, i));
        writer.write(StringUtils.format("%d,%s,%d", 20190102 + i, "name_" + (i + 1), i + 1));
      }
      totalFileSize += file.length();
    }

    final InputEntityIteratingReader reader = new InputEntityIteratingReader(
        new InputRowSchema(
            new TimestampSpec("time", "yyyyMMdd", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of("time", "name", "score"))
            ),
            ColumnsFilter.all()
        ),
        new CsvInputFormat(
            ImmutableList.of("time", "name", "score"),
            null,
            null,
            false,
            0
        ),
        CloseableIterators.withEmptyBaggage(
            files.stream().flatMap(file -> ImmutableList.of(new FileEntity(file)).stream()).iterator()
        ),
        SystemFieldDecoratorFactory.NONE,
        temporaryFolder.newFolder()
    );

    final InputStats inputStats = new InputStatsImpl();
    try (CloseableIterator<InputRow> iterator = reader.read(inputStats)) {
      int i = 0;
      while (iterator.hasNext()) {
        InputRow row = iterator.next();
        Assert.assertEquals(DateTimes.of(StringUtils.format("2019-01-%02d", i + 1)), row.getTimestamp());
        Assert.assertEquals(StringUtils.format("name_%d", i), Iterables.getOnlyElement(row.getDimension("name")));
        Assert.assertEquals(Integer.toString(i), Iterables.getOnlyElement(row.getDimension("score")));

        Assert.assertTrue(iterator.hasNext());
        row = iterator.next();
        Assert.assertEquals(DateTimes.of(StringUtils.format("2019-01-%02d", i + 2)), row.getTimestamp());
        Assert.assertEquals(StringUtils.format("name_%d", i + 1), Iterables.getOnlyElement(row.getDimension("name")));
        Assert.assertEquals(Integer.toString(i + 1), Iterables.getOnlyElement(row.getDimension("score")));
        i++;
      }
      Assert.assertEquals(numFiles, i);
      Assert.assertEquals(totalFileSize, inputStats.getProcessedBytes());
    }
  }

  @Test
  public void testSampleWithSystemFields() throws IOException
  {
    final int numFiles = 5;
    final List<File> files = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      final File file = temporaryFolder.newFile("test_" + i);
      files.add(file);
      try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("%d,%s,%d\n", 20190101 + i, "name_" + i, i));
        writer.write(StringUtils.format("%d,%s,%d", 20190102 + i, "name_" + (i + 1), i + 1));
      }
    }

    LocalInputSource inputSource = new LocalInputSource(
        temporaryFolder.getRoot(),
        "test_*",
        null,
        new SystemFields(EnumSet.of(SystemField.URI, SystemField.PATH)));
    final InputEntityIteratingReader reader = new InputEntityIteratingReader(
        new InputRowSchema(
            new TimestampSpec("time", "yyyyMMdd", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of(
                    "time",
                    "name",
                    "score",
                    SystemField.URI.getFieldName(),
                    SystemField.PATH.getFieldName()
                ))
            ),
            ColumnsFilter.all()
        ),
        new CsvInputFormat(
            ImmutableList.of("time", "name", "score"),
            null,
            null,
            false,
            0
        ),
        CloseableIterators.withEmptyBaggage(
            files.stream().flatMap(file -> ImmutableList.of(new FileEntity(file)).stream()).iterator()
        ),
        SystemFieldDecoratorFactory.fromInputSource(inputSource),
        temporaryFolder.newFolder()
    );

    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      int i = 0;
      while (iterator.hasNext()) {
        InputRow row = iterator.next().getInputRows().get(0);
        Assert.assertEquals(DateTimes.of(StringUtils.format("2019-01-%02d", i + 1)), row.getTimestamp());
        Assert.assertEquals(StringUtils.format("name_%d", i), Iterables.getOnlyElement(row.getDimension("name")));
        Assert.assertEquals(Integer.toString(i), Iterables.getOnlyElement(row.getDimension("score")));
        Assert.assertEquals(files.get(i).toURI().toString(), row.getDimension(SystemField.URI.getFieldName()).get(0));
        Assert.assertEquals(files.get(i).getAbsolutePath(), row.getDimension(SystemField.PATH.getFieldName()).get(0));

        Assert.assertTrue(iterator.hasNext());
        row = iterator.next().getInputRows().get(0);
        Assert.assertEquals(DateTimes.of(StringUtils.format("2019-01-%02d", i + 2)), row.getTimestamp());
        Assert.assertEquals(StringUtils.format("name_%d", i + 1), Iterables.getOnlyElement(row.getDimension("name")));
        Assert.assertEquals(Integer.toString(i + 1), Iterables.getOnlyElement(row.getDimension("score")));
        Assert.assertEquals(files.get(i).toURI().toString(), row.getDimension(SystemField.URI.getFieldName()).get(0));
        Assert.assertEquals(files.get(i).getAbsolutePath(), row.getDimension(SystemField.PATH.getFieldName()).get(0));
        i++;
      }
      Assert.assertEquals(numFiles, i);
    }
  }

  @Test
  public void testIncorrectURI() throws IOException, URISyntaxException
  {
    final InputEntityIteratingReader firehose = new InputEntityIteratingReader(
        new InputRowSchema(
            new TimestampSpec(null, null, null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of("time", "name", "score"))
            ),
            ColumnsFilter.all()
        ),
        new CsvInputFormat(
            ImmutableList.of("time", "name", "score"),
            null,
            null,
            false,
            0
        ),
        CloseableIterators.withEmptyBaggage(
            ImmutableList.of(
                new HttpEntity(new URI("testscheme://some/path"), null, null)
                {
                  @Override
                  protected int getMaxRetries()
                  {
                    // override so this test does not take like 4 minutes to run
                    return 2;
                  }
                }

            ).iterator()
        ),
        SystemFieldDecoratorFactory.NONE,
        temporaryFolder.newFolder()
    );

    try (CloseableIterator<InputRow> readIterator = firehose.read()) {
      String expectedMessage = "Error occurred while trying to read uri: testscheme://some/path";
      Exception exception = Assert.assertThrows(RuntimeException.class, readIterator::hasNext);
      Assert.assertTrue(exception.getMessage().contains(expectedMessage));
    }
  }
}
