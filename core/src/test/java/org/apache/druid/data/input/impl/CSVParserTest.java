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
import com.google.common.collect.ImmutableMap;
import com.opencsv.RFC4180Parser;
import org.apache.druid.java.util.common.parsers.CSVParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CSVParserTest
{
  private final RFC4180Parser parser = new RFC4180Parser();

  @Test
  public void testBasic() throws IOException
  {
    CSVParser parser = new CSVParser(null, ImmutableList.of("Value", "Comment", "Timestamp"), false, 0);

    final List<String> inputs = ImmutableList.of(
        "3,\"Lets do some \"\"normal\"\" quotes\",2018-05-05T10:00:00Z",
        "34,\"Lets do some \"\"normal\"\", quotes with comma\",2018-05-06T10:00:00Z",
        "343,\"Lets try \\\"\"it\\\"\" with slash quotes\",2018-05-07T10:00:00Z",
        "545,\"Lets try \\\"\"it\\\"\", with slash quotes and comma\",2018-05-08T10:00:00Z",
        "65,Here I write \\n slash n,2018-05-09T10:00:00Z"
    );
    final List<Map<String, Object>> expectedResult = ImmutableList.of(
        ImmutableMap.of("Value", "3", "Comment", "Lets do some \"normal\" quotes", "Timestamp", "2018-05-05T10:00:00Z"),
        ImmutableMap.of(
            "Value",
            "34",
            "Comment",
            "Lets do some \"normal\", quotes with comma",
            "Timestamp",
            "2018-05-06T10:00:00Z"
        ),
        ImmutableMap.of(
            "Value",
            "343",
            "Comment",
            "Lets try \\\"it\\\" with slash quotes",
            "Timestamp",
            "2018-05-07T10:00:00Z"
        ),
        ImmutableMap.of(
            "Value",
            "545",
            "Comment",
            "Lets try \\\"it\\\", with slash quotes and comma",
            "Timestamp",
            "2018-05-08T10:00:00Z"
        ),
        ImmutableMap.of("Value", "65", "Comment", "Here I write \\n slash n", "Timestamp", "2018-05-09T10:00:00Z")
    );
    final List<Map<String, Object>> parsedResult = new ArrayList<>();

    for (String input : inputs) {
      Map<String, Object> parsedLineList = parser.parseToMap(input);
      parsedResult.add(parsedLineList);
    }

    Assert.assertTrue(expectedResult.equals(parsedResult));
  }

  @Test
  public void testRussianTextMess() throws IOException
  {
    CSVParser parser = new CSVParser(null, ImmutableList.of("Comment"), false, 0);
    final String input = "\"Как говорится: \\\"\"всё течет, всё изменяется\\\"\". Украина как всегда обвиняет Россию в собственных проблемах. #ПровокацияКиева\"";
    final Map<String, Object> expect = ImmutableMap.of(
        "Comment",
        "Как говорится: \\\"всё течет, всё изменяется\\\". Украина как всегда обвиняет Россию в собственных проблемах. #ПровокацияКиева"
    );
    final Map<String, Object> parsedInput = parser.parseToMap(input);

    Assert.assertTrue(parsedInput.get("Comment").getClass().equals(String.class));
    Assert.assertTrue(expect.equals(parsedInput));
  }
}
