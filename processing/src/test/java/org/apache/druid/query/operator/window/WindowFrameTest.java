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

package org.apache.druid.query.operator.window;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.query.operator.window.WindowFrame.OffsetFrame;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WindowFrameTest
{
  @Test
  public void testEqualsRows()
  {
    EqualsVerifier.forClass(WindowFrame.Rows.class)
        .usingGetClass()
        .verify();
  }

  @Test
  public void testEqualsGroups()
  {
    EqualsVerifier.forClass(WindowFrame.Groups.class)
        .usingGetClass()
        .verify();
  }

  @Test
  public void testOffsetFrameUnbounded()
  {
    OffsetFrame of = new WindowFrame.Rows(null, null);
    assertEquals(-100, of.getLowerOffsetClamped(100));
    assertEquals(100, of.getUpperOffsetClamped(100));
  }

  @Test
  public void testOffsetFrameNormal()
  {
    OffsetFrame of = new WindowFrame.Rows(-1, 2);
    assertEquals(-1, of.getLowerOffsetClamped(100));
    assertEquals(2, of.getUpperOffsetClamped(100));
  }

  @Test
  public void testOffsetFrameUnbounded2()
  {
    OffsetFrame of = new WindowFrame.Rows(-200, 200);
    assertEquals(-100, of.getLowerOffsetClamped(100));
    assertEquals(100, of.getUpperOffsetClamped(100));
  }

}
