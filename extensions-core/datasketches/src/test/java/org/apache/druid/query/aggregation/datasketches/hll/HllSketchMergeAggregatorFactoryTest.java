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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.yahoo.sketches.hll.TgtHllType;
import org.apache.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HllSketchMergeAggregatorFactoryTest
{
  private static final String NAME = "name";
  private static final String FIELD_NAME = "fieldName";
  private static final int LG_K = 2;
  private static final String TGT_HLL_TYPE = TgtHllType.HLL_6.name();
  private static final boolean ROUND = true;

  private HllSketchMergeAggregatorFactory targetRound;
  private HllSketchMergeAggregatorFactory targetNoRound;

  @Before
  public void setUp()
  {
    targetRound = new HllSketchMergeAggregatorFactory(NAME, FIELD_NAME, LG_K, TGT_HLL_TYPE, ROUND);
    targetNoRound = new HllSketchMergeAggregatorFactory(NAME, FIELD_NAME, LG_K, TGT_HLL_TYPE, !ROUND);
  }

  @Test(expected = AggregatorFactoryNotMergeableException.class)
  public void testGetMergingFactory_badName() throws Exception
  {
    HllSketchMergeAggregatorFactory other = new HllSketchMergeAggregatorFactory(
        NAME + "-diff",
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        ROUND
    );
    targetRound.getMergingFactory(other);
  }

  @Test(expected = AggregatorFactoryNotMergeableException.class)
  public void testGetMergingFactory_badType() throws Exception
  {
    HllSketchBuildAggregatorFactory other = new HllSketchBuildAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        ROUND
    );
    targetRound.getMergingFactory(other);
  }

  @Test
  public void testGetMergingFactory_otherSmallerLgK() throws Exception
  {
    final int smallerLgK = LG_K - 1;
    HllSketchMergeAggregatorFactory other = new HllSketchMergeAggregatorFactory(
        NAME,
        FIELD_NAME,
        smallerLgK,
        TGT_HLL_TYPE,
        ROUND
    );
    HllSketchAggregatorFactory result = (HllSketchAggregatorFactory) targetRound.getMergingFactory(other);
    assertEquals(LG_K, result.getLgK());
  }

  @Test
  public void testGetMergingFactory_otherLargerLgK() throws Exception
  {
    final int largerLgK = LG_K + 1;
    HllSketchMergeAggregatorFactory other = new HllSketchMergeAggregatorFactory(
        NAME,
        FIELD_NAME,
        largerLgK,
        TGT_HLL_TYPE,
        ROUND
    );
    HllSketchAggregatorFactory result = (HllSketchAggregatorFactory) targetRound.getMergingFactory(other);
    assertEquals(largerLgK, result.getLgK());
  }

  @Test
  public void testGetMergingFactory_otherSmallerTgtHllType() throws Exception
  {
    String smallerTgtHllType = TgtHllType.HLL_4.name();
    HllSketchMergeAggregatorFactory other = new HllSketchMergeAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        smallerTgtHllType,
        ROUND
    );
    HllSketchAggregatorFactory result = (HllSketchAggregatorFactory) targetRound.getMergingFactory(other);
    assertEquals(TGT_HLL_TYPE, result.getTgtHllType());
  }

  @Test
  public void testGetMergingFactory_otherLargerTgtHllType() throws Exception
  {
    String largerTgtHllType = TgtHllType.HLL_8.name();
    HllSketchMergeAggregatorFactory other = new HllSketchMergeAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        largerTgtHllType,
        ROUND
    );
    HllSketchAggregatorFactory result = (HllSketchAggregatorFactory) targetRound.getMergingFactory(other);
    assertEquals(largerTgtHllType, result.getTgtHllType());
  }

  @Test
  public void testGetMergingFactory_thisNoRound_otherNoRound() throws Exception
  {
    HllSketchAggregatorFactory result = (HllSketchAggregatorFactory) targetNoRound.getMergingFactory(targetNoRound);
    assertFalse(result.isRound());
  }

  @Test
  public void testGetMergingFactory_thisNoRound_otherRound() throws Exception
  {
    HllSketchAggregatorFactory result = (HllSketchAggregatorFactory) targetNoRound.getMergingFactory(targetRound);
    assertTrue(result.isRound());
  }

  @Test
  public void testGetMergingFactory_thisRound_otherNoRound() throws Exception
  {
    HllSketchAggregatorFactory result = (HllSketchAggregatorFactory) targetRound.getMergingFactory(targetNoRound);
    assertTrue(result.isRound());
  }

  @Test
  public void testGetMergingFactory_thisRound_otherRound() throws Exception
  {
    HllSketchAggregatorFactory result = (HllSketchAggregatorFactory) targetRound.getMergingFactory(targetRound);
    assertTrue(result.isRound());
  }
}
