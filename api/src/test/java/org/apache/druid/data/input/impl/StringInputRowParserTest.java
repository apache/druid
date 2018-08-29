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
import org.apache.druid.js.JavaScriptConfig;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class StringInputRowParserTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testDisableJavaScript()
  {
    final JavaScriptParseSpec parseSpec = new JavaScriptParseSpec(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(
                ImmutableList.of(
                    "dim1",
                    "dim2"
                )
            ),
            null,
            null
        ),
        "func",
        new JavaScriptConfig(false)
    );
    final StringInputRowParser parser = new StringInputRowParser(parseSpec, "UTF-8");

    expectedException.expect(CoreMatchers.instanceOf(IllegalStateException.class));
    expectedException.expectMessage("JavaScript is disabled");

    parser.startFileFromBeginning();
  }

  @Test
  public void testDisableJavaScript2()
  {
    final JavaScriptParseSpec parseSpec = new JavaScriptParseSpec(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(
                ImmutableList.of(
                    "dim1",
                    "dim2"
                )
            ),
            null,
            null
        ),
        "func",
        new JavaScriptConfig(false)
    );
    final StringInputRowParser parser = new StringInputRowParser(parseSpec, "UTF-8");

    expectedException.expect(CoreMatchers.instanceOf(IllegalStateException.class));
    expectedException.expectMessage("JavaScript is disabled");

    parser.parse("");
  }
}
