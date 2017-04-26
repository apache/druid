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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.java.util.common.Pair;
import org.apache.commons.io.LineIterator;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

public class FileIteratingFirehoseTest
{
  private static final List<Pair<String[], ImmutableList<String>>> fixtures = ImmutableList.of(
      Pair.of(new String[]{"2000,foo"}, ImmutableList.of("foo")),
      Pair.of(new String[]{"2000,foo\n2000,bar\n"}, ImmutableList.of("foo", "bar")),
      Pair.of(new String[]{"2000,foo\n2000,bar\n", "2000,baz"}, ImmutableList.of("foo", "bar", "baz")),
      Pair.of(new String[]{"2000,foo\n2000,bar\n", "", "2000,baz"}, ImmutableList.of("foo", "bar", "baz")),
      Pair.of(new String[]{"2000,foo\n2000,bar\n", "", "2000,baz", ""}, ImmutableList.of("foo", "bar", "baz")),
      Pair.of(new String[]{""}, ImmutableList.<String>of()),
      Pair.of(new String[]{}, ImmutableList.<String>of())
  );

  private static final StringInputRowParser parser = new StringInputRowParser(
      new CSVParseSpec(
          new TimestampSpec("ts", "auto", null),
          new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("x")), null, null),
          ",",
          ImmutableList.of("ts", "x")
      ),
      null
  );

  @Test
  public void testFirehose() throws Exception
  {
    for (Pair<String[], ImmutableList<String>> fixture : fixtures) {
      final List<LineIterator> lineIterators = Lists.transform(
          Arrays.asList(fixture.lhs),
          new Function<String, LineIterator>()
          {
            @Override
            public LineIterator apply(String s)
            {
              return new LineIterator(new StringReader(s));
            }
          }
      );

      final FileIteratingFirehose firehose = new FileIteratingFirehose(lineIterators.iterator(), parser);
      final List<String> results = Lists.newArrayList();

      while (firehose.hasMore()) {
        results.add(Joiner.on("|").join(firehose.nextRow().getDimension("x")));
      }

      Assert.assertEquals(fixture.rhs, results);
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
        final char[] chs = "\n".toCharArray();
        System.arraycopy(chs, 0, cbuf, 0, chs.length);
        return chs.length;
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
