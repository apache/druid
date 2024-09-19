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

package org.apache.druid.java.util.common.parsers;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.druid.java.util.common.parsers.ParserUtils.findDuplicates;
import static org.apache.druid.java.util.common.parsers.ParserUtils.getTransformationFunction;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ParserUtilsTest
{
  static {
    NullHandling.initializeForTests();
  }

  @Test
  public void testFindDuplicatesMixedCases()
  {
    final List<String> fields = ImmutableList.of("f1", "f2", "F1", "F2", "f3");
    assertEquals(Collections.emptySet(), findDuplicates(fields));
  }

  @Test
  public void testFindDuplicates()
  {
    final List<String> fields = ImmutableList.of("f1", "f2", "F1", "F2", "f1", "F2");
    assertEquals(ImmutableSet.of("f1", "F2"), findDuplicates(fields));
  }

  @Test
  public void testInputWithDelimiterAndParserDisabled()
  {
    assertNull(
        getTransformationFunction("|", Splitter.on("|"), true).apply(null)
    );
    assertEquals(
        NullHandling.emptyToNullIfNeeded(""),
        getTransformationFunction("|", Splitter.on("|"), true).apply("")
    );
    assertEquals(
        ImmutableList.of("foo", "boo"),
        getTransformationFunction("|", Splitter.on("|"), false).apply("foo|boo")
    );
    assertEquals(
        ImmutableList.of("1", "2", "3"),
        getTransformationFunction("|", Splitter.on("|"), false).apply("1|2|3")
    );
    assertEquals(
        ImmutableList.of("1", "-2", "3", "0", "-2"),
        getTransformationFunction("|", Splitter.on("|"), false).apply("1|-2|3|0|-2")
    );
    assertEquals(
        "100",
        getTransformationFunction("|", Splitter.on("|"), false).apply("100")
    );
    assertEquals(
        "1.23",
        getTransformationFunction("|", Splitter.on("|"), false).apply("1.23")
    );
    assertEquals(
        "-2.0",
        getTransformationFunction("|", Splitter.on("|"), false).apply("-2.0")
    );
    assertEquals(
        "1e2",
        getTransformationFunction("|", Splitter.on("|"), false).apply("1e2")
    );
    assertEquals(
        ImmutableList.of("1", "2", "3"),
        getTransformationFunction("|", Splitter.on("|"), false).apply("1|2|3")
    );
    assertEquals(
        ImmutableList.of("1", "-2", "3", "0", "-2"),
        getTransformationFunction("|", Splitter.on("|"), false).apply("1|-2|3|0|-2")
    );
    assertEquals(
        ImmutableList.of("-1.0", "-2.2", "3.1", "0.2", "-2.1"),
        getTransformationFunction("|", Splitter.on("|"), false).apply("-1.0|-2.2|3.1|0.2|-2.1")
    );

    // Some mixed types
    assertEquals(
        ImmutableList.of("-1.23", "3.13", "23"),
        getTransformationFunction("|", Splitter.on("|"), false).apply("-1.23|3.13|23")
    );
    assertEquals(
        ImmutableList.of("-1.23", "3.13", "23", "foo", "-9"),
        getTransformationFunction("|", Splitter.on("|"), false).apply("-1.23|3.13|23|foo|-9")
    );
  }

  @Test
  public void testInputWithDelimiterAndParserEnabled()
  {
    assertNull(
        getTransformationFunction("|", Splitter.on("|"), true).apply(null)
    );
    assertEquals(
        NullHandling.emptyToNullIfNeeded(""),
        getTransformationFunction("|", Splitter.on("|"), true).apply("")
    );
    assertEquals(
        ImmutableList.of("foo", "boo"),
        getTransformationFunction("|", Splitter.on("|"), true).apply("foo|boo")
    );
    assertEquals(
        ImmutableList.of(1L, 2L, 3L),
        getTransformationFunction("|", Splitter.on("|"), true).apply("1|2|3")
    );
    assertEquals(
        ImmutableList.of(1L, -2L, 3L, 0L, -2L),
        getTransformationFunction("|", Splitter.on("|"), true).apply("1|-2|3|0|-2")
    );
    assertEquals(
        100L,
        getTransformationFunction("|", Splitter.on("|"), true).apply("100")
    );
    assertEquals(
        1.23,
        getTransformationFunction("|", Splitter.on("|"), true).apply("1.23")
    );
    assertEquals(
        -2.0,
        getTransformationFunction("|", Splitter.on("|"), true).apply("-2.0")
    );
    assertEquals(
        100.0,
        getTransformationFunction("$", Splitter.on("|"), true).apply("1e2")
    );
    assertEquals(
        ImmutableList.of(1L, 2L, 3L),
        getTransformationFunction("|", Splitter.on("|"), true).apply("1|2|3")
    );
    assertEquals(
        ImmutableList.of(1L, -2L, 3L, 0L, -2L),
        getTransformationFunction("|", Splitter.on("|"), true).apply("1|-2|3|0|-2")
    );
    assertEquals(
        ImmutableList.of(-1.0, -2.2, 3.1, 0.2, -2.1),
        getTransformationFunction("|", Splitter.on("|"), true).apply("-1.0|-2.2|3.1|0.2|-2.1")
    );

    // Some mixed types
    assertEquals(
        ImmutableList.of(-1.23, 3.13, 23L),
        getTransformationFunction("|", Splitter.on("|"), true).apply("-1.23|3.13|23")
    );
    assertEquals(
        ImmutableList.of(-1.23, 3.13, 23L, "foo", -9L),
        getTransformationFunction("|", Splitter.on("|"), true).apply("-1.23|3.13|23|foo|-9")
    );
  }

  @Test
  public void testInputWithoutDelimiterAndNumberParsingDisabled()
  {
    assertNull(
        getTransformationFunction("|", Splitter.on("$"), false).apply(null)
    );
    assertEquals(
        NullHandling.emptyToNullIfNeeded(""),
        getTransformationFunction("|", Splitter.on("$"), false).apply("")
    );
    assertEquals(
        "foo|boo",
        getTransformationFunction("$", Splitter.on("$"), false).apply("foo|boo")
    );
    assertEquals(
        "100",
        getTransformationFunction("$", Splitter.on("$"), false).apply("100")
    );
    assertEquals(
        "1.23",
        getTransformationFunction("$", Splitter.on("$"), false).apply("1.23")
    );
    assertEquals(
        "-2.0",
        getTransformationFunction("$", Splitter.on("$"), false).apply("-2.0")
    );
    assertEquals(
        "1e2",
        getTransformationFunction("$", Splitter.on("$"), false).apply("1e2")
    );
    assertEquals(
        "1|2|3",
        getTransformationFunction("$", Splitter.on("$"), false).apply("1|2|3")
    );
    assertEquals(
        "1|-2|3|0|-2",
        getTransformationFunction("$", Splitter.on("$"), false).apply("1|-2|3|0|-2")
    );
    assertEquals(
        "-1.0|-2.2|3.1|0.2|-2.1",
        getTransformationFunction("$", Splitter.on("$"), false).apply("-1.0|-2.2|3.1|0.2|-2.1")
    );

    // Some mixed types
    assertEquals(
        "-1.23|3.13|23",
        getTransformationFunction("$", Splitter.on("$"), false).apply("-1.23|3.13|23")
    );
    assertEquals(
        "-1.23|3.13|23|foo|-9",
        getTransformationFunction("$", Splitter.on("$"), false).apply("-1.23|3.13|23|foo|-9")
    );
  }

  @Test
  public void testInputWithoutDelimiterAndNumberParsingEnabled()
  {
    assertNull(
        getTransformationFunction("$", Splitter.on("$"), true).apply(null)
    );
    assertEquals(
        NullHandling.emptyToNullIfNeeded(""),
        getTransformationFunction("$", Splitter.on("$"), true).apply("")
    );
    assertEquals(
        "foo|boo",
        getTransformationFunction("$", Splitter.on("$"), true).apply("foo|boo")
    );
    assertEquals(
        100L,
        getTransformationFunction("$", Splitter.on("$"), true).apply("100")
    );
    assertEquals(
        1.23,
        getTransformationFunction("$", Splitter.on("$"), true).apply("1.23")
    );
    assertEquals(
        -2.0,
        getTransformationFunction("$", Splitter.on("$"), true).apply("-2.0")
    );
    assertEquals(
        100.0,
        getTransformationFunction("$", Splitter.on("$"), true).apply("1e2")
    );
    assertEquals(
        "1|2|3",
        getTransformationFunction("$", Splitter.on("$"), true).apply("1|2|3")
    );
    assertEquals(
        "1|-2|3|0|-2",
        getTransformationFunction("$", Splitter.on("$"), true).apply("1|-2|3|0|-2")
    );
    assertEquals(
        "-1.0|-2.2|3.1|0.2|-2.1",
        getTransformationFunction("$", Splitter.on("$"), true).apply("-1.0|-2.2|3.1|0.2|-2.1")
    );

    // Some mixed types
    assertEquals(
        "-1.23|3.13|23",
        getTransformationFunction("$", Splitter.on("$"), true).apply("-1.23|3.13|23")
    );
    assertEquals(
        "-1.23|3.13|23|foo|-9",
        getTransformationFunction("$", Splitter.on("$"), true).apply("-1.23|3.13|23|foo|-9")
    );
  }
}
