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
import junit.framework.Assert;

import org.apache.commons.io.LineIterator;
import org.junit.Test;

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

      final StringInputRowParser parser = new StringInputRowParser(
          new CSVParseSpec(
              new TimestampSpec("ts", "auto", null),
              new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("x")), null, null),
              ",",
              ImmutableList.of("ts", "x")
          ),
          null
      );

      final FileIteratingFirehose firehose = new FileIteratingFirehose(lineIterators.iterator(), parser);
      final List<String> results = Lists.newArrayList();

      while (firehose.hasMore()) {
        results.add(Joiner.on("|").join(firehose.nextRow().getDimension("x")));
      }

      Assert.assertEquals(fixture.rhs, results);
    }
  }
}
