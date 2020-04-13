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

package org.apache.druid.data.input.influx;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.Parser;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

@RunWith(JUnitParamsRunner.class)
public class InfluxParserTest
{
  @SuppressWarnings("unused")
  private String name;
  @SuppressWarnings("unused")
  private String input;
  @SuppressWarnings("unused")
  private Map<String, Object> expected;

  private static Object[] testCase(String name, String input, Parsed expected)
  {
    return Lists.newArrayList(name, input, expected).toArray();
  }

  public Object[] testData()
  {
    return Lists.newArrayList(
        testCase(
            "real sample",
            "cpu,host=foo.bar.baz,region=us-east-1,application=echo pct_idle=99.3,pct_user=88.8,m1_load=2i 1465839830100400200",
            Parsed.row("cpu", 1465839830100L)
                  .with("host", "foo.bar.baz")
                  .with("region", "us-east-1")
                  .with("application", "echo")
                  .with("pct_idle", 99.3)
                  .with("pct_user", 88.8)
                  .with("m1_load", 2L)
        ),
        testCase(
            "negative timestamp",
            "foo,region=us-east-1,host=127.0.0.1 m=1.0,n=3.0,o=500i -123456789",
            Parsed.row("foo", -123L)
                  .with("region", "us-east-1")
                  .with("host", "127.0.0.1")
                  .with("m", 1.0)
                  .with("n", 3.0)
                  .with("o", 500L)
        ),
        testCase(
            "truncated timestamp",
            "foo,region=us-east-1,host=127.0.0.1 m=1.0,n=3.0,o=500i 123",
            Parsed.row("foo", 0L)
                  .with("region", "us-east-1")
                  .with("host", "127.0.0.1")
                  .with("m", 1.0)
                  .with("n", 3.0)
                  .with("o", 500L)
        ),
        testCase(
            "special characters",
            "!@#$%^&*()_-\\=+,+++\\ +++=--\\ --- __**__=\"ü\" 123456789",
            Parsed.row("!@#$%^&*()_-=+", 123L)
                  .with("+++ +++", "-- ---")
                  .with("__**__", "127.0.0.1")
                  .with("__**__", "ü")
        ),
        testCase(
            "unicode characters",
            "\uD83D\uDE00,\uD83D\uDE05=\uD83D\uDE06 \uD83D\uDE0B=100i,b=\"\uD83D\uDE42\" 123456789",
            Parsed.row("\uD83D\uDE00", 123L)
                  .with("\uD83D\uDE05", "\uD83D\uDE06")
                  .with("\uD83D\uDE0B", 100L)
                  .with("b", "\uD83D\uDE42")
        ),
        testCase(
            "quoted string measurement value",
            "foo,region=us-east-1,host=127.0.0.1 m=1.0,n=3.0,o=\"something \\\"cool\\\" \" 123456789",
            Parsed.row("foo", 123L)
                  .with("region", "us-east-1")
                  .with("host", "127.0.0.1")
                  .with("m", 1.0)
                  .with("n", 3.0)
                  .with("o", "something \"cool\" ")
        ),
        testCase(
            "no tags",
            "foo m=1.0,n=3.0 123456789",
            Parsed.row("foo", 123L)
                  .with("m", 1.0)
                  .with("n", 3.0)
        ),
        testCase(
            "Escaped characters in identifiers",
            "f\\,oo\\ \\=,bar=baz m=1.0,n=3.0 123456789",
            Parsed.row("f,oo =", 123L)
                  .with("bar", "baz")
                  .with("m", 1.0)
                  .with("n", 3.0)
        ),
        testCase(
            "Escaped characters in identifiers",
            "foo\\ \\=,bar=baz m=1.0,n=3.0 123456789",
            Parsed.row("foo =", 123L)
                  .with("bar", "baz")
                  .with("m", 1.0)
                  .with("n", 3.0)
        )
    ).toArray();
  }

  @Test
  @Parameters(method = "testData")
  public void testParse(String name, String input, Parsed expected)
  {
    Parser<String, Object> parser = new InfluxParser(null);
    Map<String, Object> parsed = parser.parseToMap(input);
    MatcherAssert.assertThat(
        "correct measurement name",
        parsed.get("measurement"),
        Matchers.equalTo(expected.measurement)
    );
    MatcherAssert.assertThat(
        "correct timestamp",
        parsed.get(InfluxParser.TIMESTAMP_KEY),
        Matchers.equalTo(expected.timestamp)
    );
    expected.kv.forEach((k, v) -> MatcherAssert.assertThat("correct field " + k, parsed.get(k), Matchers.equalTo(v)));
    parsed.remove("measurement");
    parsed.remove(InfluxParser.TIMESTAMP_KEY);
    MatcherAssert.assertThat("No extra keys in parsed data", parsed.keySet(), Matchers.equalTo(expected.kv.keySet()));
  }

  @Test
  public void testParseWhitelistPass()
  {
    Parser<String, Object> parser = new InfluxParser(Sets.newHashSet("cpu"));
    String input = "cpu,host=foo.bar.baz,region=us-east,application=echo pct_idle=99.3,pct_user=88.8,m1_load=2 1465839830100400200";
    Map<String, Object> parsed = parser.parseToMap(input);
    MatcherAssert.assertThat(parsed.get("measurement"), Matchers.equalTo("cpu"));
  }

  @Test
  public void testParseWhitelistFail()
  {
    Parser<String, Object> parser = new InfluxParser(Sets.newHashSet("mem"));
    String input = "cpu,host=foo.bar.baz,region=us-east,application=echo pct_idle=99.3,pct_user=88.8,m1_load=2 1465839830100400200";
    try {
      parser.parseToMap(input);
    }
    catch (ParseException t) {
      MatcherAssert.assertThat(t, Matchers.isA(ParseException.class));
      return;
    }

    Assert.fail("Exception not thrown");
  }

  public Object[] failureTestData()
  {
    return Lists.newArrayList(
        Pair.of("Empty line", ""),
        Pair.of("Invalid measurement", "invalid measurement"),
        Pair.of("Invalid timestamp", "foo i=123 123x")
    ).toArray();
  }

  @Test
  @Parameters(method = "failureTestData")
  public void testParseFailures(Pair<String, String> testCase)
  {
    Parser<String, Object> parser = new InfluxParser(null);
    try {
      parser.parseToMap(testCase.rhs);
    }
    catch (ParseException t) {
      MatcherAssert.assertThat(t, Matchers.isA(ParseException.class));
      return;
    }

    Assert.fail(testCase.rhs + ": exception not thrown");
  }

  private static class Parsed
  {
    private String measurement;
    private Long timestamp;
    private final Map<String, Object> kv = new HashMap<>();

    static Parsed row(String measurement, Long timestamp)
    {
      Parsed e = new Parsed();
      e.measurement = measurement;
      e.timestamp = timestamp;
      return e;
    }

    Parsed with(String k, Object v)
    {
      kv.put(k, v);
      return this;
    }
  }
}
