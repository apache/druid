/*
 * Copyright 2011 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.common.parsers;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Map;

public class DelimitedParserTest
{

  @Test
  public void testValidHeader()
  {
    String tsv = "time\tvalue1\tvalue2";
    final Parser<String, Object> delimitedParser;
    boolean parseable = true;
    try {
      delimitedParser = new DelimitedParser(Optional.of("\t"), Optional.<String>absent(), tsv);
    }
    catch (Exception e) {
      parseable = false;
    }
    finally {
      Assert.assertTrue(parseable);
    }
  }

  @Test
  public void testInvalidHeader()
  {
    String tsv = "time\tvalue1\tvalue2\tvalue2";
    final Parser<String, Object> delimitedParser;
    boolean parseable = true;
    try {
      delimitedParser = new DelimitedParser(Optional.of("\t"), Optional.<String>absent(), tsv);
    }
    catch (Exception e) {
      parseable = false;
    }
    finally {
      Assert.assertFalse(parseable);
    }
  }

  @Test
  public void testTSVParserWithHeader()
  {
    String header = "time\tvalue1\tvalue2";
    final Parser<String, Object> delimitedParser = new DelimitedParser(Optional.of("\t"), Optional.<String>absent(), header);
    String body = "hello\tworld\tfoo";
    final Map<String, Object> jsonMap = delimitedParser.parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap
    );
  }

  @Test
  public void testTSVParserWithoutHeader()
  {
    final Parser<String, Object> delimitedParser = new DelimitedParser(Optional.of("\t"), Optional.<String>absent());
    String body = "hello\tworld\tfoo";
    final Map<String, Object> jsonMap = delimitedParser.parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("column_1", "hello", "column_2", "world", "column_3", "foo"),
        jsonMap
    );
  }
}
