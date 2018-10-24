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

package org.apache.druid.segment.realtime.firehose;

import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class LocalFirehoseFactoryTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private LocalFirehoseFactory factory;

  @Before
  public void setup() throws IOException
  {
    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
          Files.newBufferedWriter(temporaryFolder.newFile("test_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write((20171225 + i) + "," + i + "th test file\n");
      }
    }

    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
          Files.newBufferedWriter(temporaryFolder.newFile("filtered_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write((20171225 + i) + "," + i + "th filtered file\n");
      }
    }

    factory = new LocalFirehoseFactory(temporaryFolder.getRoot(), "test_*", null);
  }

  @Test
  public void testConnect() throws IOException
  {
    try (final Firehose firehose = factory.connect(new StringInputRowParser(
        new CSVParseSpec(
            new TimestampSpec(
                "timestamp",
                "auto",
                null
            ),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(Arrays.asList("timestamp", "a")),
                new ArrayList<>(),
                new ArrayList<>()
            ),
            ",",
            Arrays.asList("timestamp", "a"),
            false,
            0
        ),
        StandardCharsets.UTF_8.name()
    ), null)) {
      final List<Row> rows = new ArrayList<>();
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }

      Assert.assertEquals(5, rows.size());
      rows.sort(Comparator.comparing(Row::getTimestamp));
      for (int i = 0; i < 5; i++) {
        final List<String> dimVals = rows.get(i).getDimension("a");
        Assert.assertEquals(1, dimVals.size());
        Assert.assertEquals(i + "th test file", dimVals.get(0));
      }
    }
  }
}
