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

package io.druid.query.metadata.metadata;

import org.junit.Assert;
import org.junit.Test;

public class ColumnAnalysisTest
{
  @Test
  public void testFoldStringColumns()
  {
    final ColumnAnalysis analysis1 = new ColumnAnalysis("STRING", false, 1L, 2, null);
    final ColumnAnalysis analysis2 = new ColumnAnalysis("STRING", true, 3L, 4, null);
    final ColumnAnalysis expected = new ColumnAnalysis("STRING", true, 4L, 4, null);
    Assert.assertEquals(expected, analysis1.fold(analysis2));
    Assert.assertEquals(expected, analysis2.fold(analysis1));
  }

  @Test
  public void testFoldWithNull()
  {
    final ColumnAnalysis analysis1 = new ColumnAnalysis("STRING", false, 1L, 2, null);
    Assert.assertEquals(analysis1, analysis1.fold(null));
  }

  @Test
  public void testFoldComplexColumns()
  {
    final ColumnAnalysis analysis1 = new ColumnAnalysis("hyperUnique", false, 0L, null, null);
    final ColumnAnalysis analysis2 = new ColumnAnalysis("hyperUnique", false, 0L, null, null);
    final ColumnAnalysis expected = new ColumnAnalysis("hyperUnique", false, 0L, null, null);
    Assert.assertEquals(expected, analysis1.fold(analysis2));
    Assert.assertEquals(expected, analysis2.fold(analysis1));
  }

  @Test
  public void testFoldDifferentTypes()
  {
    final ColumnAnalysis analysis1 = new ColumnAnalysis("hyperUnique", false, 1L, 1, null);
    final ColumnAnalysis analysis2 = new ColumnAnalysis("COMPLEX", false, 2L, 2, null);
    final ColumnAnalysis expected = new ColumnAnalysis("STRING", false, -1L, null, "error:cannot_merge_diff_types");
    Assert.assertEquals(expected, analysis1.fold(analysis2));
    Assert.assertEquals(expected, analysis2.fold(analysis1));
  }

  @Test
  public void testFoldSameErrors()
  {
    final ColumnAnalysis analysis1 = ColumnAnalysis.error("foo");
    final ColumnAnalysis analysis2 = ColumnAnalysis.error("foo");
    final ColumnAnalysis expected = new ColumnAnalysis("STRING", false, -1L, null, "error:foo");
    Assert.assertEquals(expected, analysis1.fold(analysis2));
    Assert.assertEquals(expected, analysis2.fold(analysis1));
  }

  @Test
  public void testFoldErrorAndNoError()
  {
    final ColumnAnalysis analysis1 = ColumnAnalysis.error("foo");
    final ColumnAnalysis analysis2 = new ColumnAnalysis("STRING", false, 2L, 2, null);
    final ColumnAnalysis expected = new ColumnAnalysis("STRING", false, -1L, null, "error:foo");
    Assert.assertEquals(expected, analysis1.fold(analysis2));
    Assert.assertEquals(expected, analysis2.fold(analysis1));
  }

  @Test
  public void testFoldDifferentErrors()
  {
    final ColumnAnalysis analysis1 = ColumnAnalysis.error("foo");
    final ColumnAnalysis analysis2 = ColumnAnalysis.error("bar");
    final ColumnAnalysis expected = new ColumnAnalysis("STRING", false, -1L, null, "error:multiple_errors");
    Assert.assertEquals(expected, analysis1.fold(analysis2));
    Assert.assertEquals(expected, analysis2.fold(analysis1));
  }
}
