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

package org.apache.druid.segment.incremental;

import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class IncrementalIndexAddResultTest
{
  @Test
  public void testIsRowAdded()
  {
    Assert.assertTrue(new IncrementalIndexAddResult(0, 0L).isRowAdded());
    Assert.assertFalse(new IncrementalIndexAddResult(0, 0L, "test").isRowAdded());
    Assert.assertFalse(new IncrementalIndexAddResult(0, 0L, new ParseException("test")).isRowAdded());
  }

  @Test
  public void testHasParseException()
  {
    Assert.assertFalse(new IncrementalIndexAddResult(0, 0L).hasParseException());
    Assert.assertFalse(new IncrementalIndexAddResult(0, 0L, "test").hasParseException());
    Assert.assertTrue(new IncrementalIndexAddResult(0, 0L, new ParseException("test")).hasParseException());
  }
}
