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

package org.apache.druid.data.input;

import com.google.common.collect.Iterables;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class FirehoseFactoryToInputSourceAdaptorTest
{
  @Test
  public void testUnimplementedInputFormat() throws IOException
  {
    final List<String> lines = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      lines.add(StringUtils.format("%d,name_%d,%d", 20190101 + i, i, i + 100));
    }
    final TestFirehoseFactory firehoseFactory = new TestFirehoseFactory(lines);
    final StringInputRowParser inputRowParser = new StringInputRowParser(
        new CSVParseSpec(
            new TimestampSpec(null, "yyyyMMdd", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("timestamp", "name", "score"))),
            ",",
            Arrays.asList("timestamp", "name", "score"),
            false,
            0
        ),
        StringUtils.UTF8_STRING
    );
    final FirehoseFactoryToInputSourceAdaptor inputSourceAdaptor = new FirehoseFactoryToInputSourceAdaptor(
        firehoseFactory,
        inputRowParser
    );
    final InputSourceReader reader = inputSourceAdaptor.reader(
        new InputRowSchema(
            inputRowParser.getParseSpec().getTimestampSpec(),
            inputRowParser.getParseSpec().getDimensionsSpec(),
            Collections.emptyList()
        ),
        null,
        null
    );
    final List<InputRow> result = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      while (iterator.hasNext()) {
        result.add(iterator.next());
      }
    }
    Assert.assertEquals(10, result.size());
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(DateTimes.of(StringUtils.format("2019-01-%02d", 1 + i)), result.get(i).getTimestamp());
      Assert.assertEquals(
          StringUtils.format("name_%d", i),
          Iterables.getOnlyElement(result.get(i).getDimension("name"))
      );
      Assert.assertEquals(
          StringUtils.format("%d", i + 100),
          Iterables.getOnlyElement(result.get(i).getDimension("score"))
      );
    }
  }

  private static class TestFirehoseFactory implements FiniteFirehoseFactory<StringInputRowParser, Object>
  {
    private final List<String> lines;

    private TestFirehoseFactory(List<String> lines)
    {
      this.lines = lines;
    }

    @Override
    public Firehose connect(StringInputRowParser parser, @Nullable File temporaryDirectory) throws ParseException
    {
      return new Firehose()
      {
        final Iterator<String> iterator = lines.iterator();

        @Override
        public boolean hasMore()
        {
          return iterator.hasNext();
        }

        @Override
        public InputRow nextRow()
        {
          return parser.parse(iterator.next());
        }

        @Override
        public void close()
        {
          // do nothing
        }
      };
    }

    @Override
    public boolean isSplittable()
    {
      return false;
    }

    @Override
    public Stream<InputSplit<Object>> getSplits(@Nullable SplitHintSpec splitHintSpec)
    {
      return null;
    }

    @Override
    public int getNumSplits(@Nullable SplitHintSpec splitHintSpec)
    {
      return 0;
    }

    @Override
    public FiniteFirehoseFactory withSplit(InputSplit split)
    {
      return null;
    }
  }
}
