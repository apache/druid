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

package org.apache.druid.common.utils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class IdUtilsTest
{
  private static final String THINGO = "thingToValidate";
  public static final String VALID_ID_CHARS = "alpha123..*~!@#&%^&*()-+ Россия\\ 한국 中国!";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testValidIdName()
  {
    IdUtils.validateId(THINGO, VALID_ID_CHARS);
  }

  @Test
  public void testInvalidNull()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("thingToValidate cannot be null or empty. Please provide a thingToValidate.");
    IdUtils.validateId(THINGO, null);
  }

  @Test
  public void testInvalidEmpty()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("thingToValidate cannot be null or empty. Please provide a thingToValidate.");
    IdUtils.validateId(THINGO, "");
  }

  @Test
  public void testInvalidSlashes()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("thingToValidate cannot contain the '/' character.");
    IdUtils.validateId(THINGO, "/paths/are/bad/since/we/make/files/from/stuff");
  }

  @Test
  public void testInvalidLeadingDot()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("thingToValidate cannot start with the '.' character.");
    IdUtils.validateId(THINGO, "./nice/try");
  }

  @Test
  public void testInvalidSpacesRegexTabs()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("thingToValidate cannot contain whitespace character except space.");
    IdUtils.validateId(THINGO, "spaces\tare\tbetter\tthan\ttabs\twhich\tare\tillegal");
  }

  @Test
  public void testInvalidSpacesRegexNewline()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("thingToValidate cannot contain whitespace character except space.");
    IdUtils.validateId(THINGO, "new\nline");
  }

  @Test
  public void testInvalidSpacesRegexCarriageReturn()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("thingToValidate cannot contain whitespace character except space.");
    IdUtils.validateId(THINGO, "does\rexist\rby\ritself");
  }

  @Test
  public void testInvalidSpacesRegexLineTabulation()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("thingToValidate cannot contain whitespace character except space.");
    IdUtils.validateId(THINGO, "wtf\u000Bis line tabulation");
  }

  @Test
  public void testInvalidSpacesRegexFormFeed()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("thingToValidate cannot contain whitespace character except space.");
    IdUtils.validateId(THINGO, "form\u000cfeed?");
  }
}
