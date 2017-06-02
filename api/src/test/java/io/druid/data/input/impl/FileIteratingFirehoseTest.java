/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.input.impl;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.data.input.InputRow;
import org.apache.commons.io.LineIterator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(Parameterized.class)
public class FileIteratingFirehoseTest
{
  @Parameters(name = "{0}, {1}")
  public static Collection<Object[]> constructorFeeder() throws IOException
  {
    final List<List<String>> inputTexts = ImmutableList.of(
        ImmutableList.of("2000,foo"),
        ImmutableList.of("2000,foo\n2000,bar\n"),
        ImmutableList.of("2000,foo\n2000,bar\n", "2000,baz"),
        ImmutableList.of("2000,foo\n2000,bar\n", "", "2000,baz"),
        ImmutableList.of("2000,foo\n2000,bar\n", "", "2000,baz", ""),
        ImmutableList.of("2000,foo\n2000,bar\n2000,baz", "", "2000,baz", "2000,foo\n2000,bar\n3000,baz"),
        ImmutableList.of(""),
        ImmutableList.of()
    );

    final List<Object[]> args = new ArrayList<>();
    for (int numSkipHeadRows = 0; numSkipHeadRows < 3; numSkipHeadRows++) {
      for (List<String> texts : inputTexts) {
        args.add(new Object[] { texts, numSkipHeadRows });
      }
    }

    return args;
  }

  private static final char[] LINE_CHARS = "\n".toCharArray();

  private final StringInputRowParser parser;
  private final List<String> inputs;
  private final List<String> expectedResults;

  public FileIteratingFirehoseTest(List<String> texts, int numSkipHeaderRows)
  {
    parser = new StringInputRowParser(
        new CSVParseSpec(
            new TimestampSpec("ts", "auto", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("x")), null, null),
            ",",
            ImmutableList.of("ts", "x"),
            false,
            numSkipHeaderRows
        ),
        null
    );

    this.inputs = texts;
    this.expectedResults = inputs.stream()
        .map(input -> input.split("\n"))
        .flatMap(lines -> {
          final List<String> filteredLines = Arrays.asList(lines).stream()
              .filter(line -> line.length() > 0)
              .map(line -> line.split(",")[1])
              .collect(Collectors.toList());

          final int numRealSkippedRows = Math.min(filteredLines.size(), numSkipHeaderRows);
          IntStream.range(0, numRealSkippedRows).forEach(i -> filteredLines.set(i, null));
          return filteredLines.stream();
        })
        .collect(Collectors.toList());
  }

  @Test
  public void testFirehose() throws Exception
  {
    final List<LineIterator> lineIterators = inputs.stream()
        .map(s -> new LineIterator(new StringReader(s)))
        .collect(Collectors.toList());

    try (final FileIteratingFirehose firehose = new FileIteratingFirehose(lineIterators.iterator(), parser)) {
      final List<String> results = Lists.newArrayList();

      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();
        if (inputRow == null) {
          results.add(null);
        } else {
          results.add(Joiner.on("|").join(inputRow.getDimension("x")));
        }
      }

      Assert.assertEquals(expectedResults, results);
    }
  }

  @Test(expected = RuntimeException.class)
  public void testClose() throws IOException
  {
    final LineIterator lineIterator = new LineIterator(new Reader()
    {
      @Override
      public int read(char[] cbuf, int off, int len) throws IOException
      {
        System.arraycopy(LINE_CHARS, 0, cbuf, 0, LINE_CHARS.length);
        return LINE_CHARS.length;
      }

      @Override
      public void close() throws IOException
      {
        throw new RuntimeException("close test for FileIteratingFirehose");
      }
    });

    final TestCloseable closeable = new TestCloseable();
    final FileIteratingFirehose firehose = new FileIteratingFirehose(
        ImmutableList.of(lineIterator).iterator(),
        parser,
        closeable
    );
    firehose.hasMore(); // initialize lineIterator
    firehose.close();
    Assert.assertTrue(closeable.closed);
  }

  private static final class TestCloseable implements Closeable
  {
    private boolean closed;

    @Override
    public void close() throws IOException
    {
      closed = true;
    }
  }
}
