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

import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
  public void testGetMergingFactoryBadName() throws Exception
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
  public void testGetMergingFactoryBadType() throws Exception
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
  public void testGetMergingFactoryOtherSmallerLgK() throws Exception
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
    Assert.assertEquals(LG_K, result.getLgK());
  }

  @Test
  public void testGetMergingFactoryOtherLargerLgK() throws Exception
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
    Assert.assertEquals(largerLgK, result.getLgK());
  }

  @Test
  public void testGetMergingFactoryOtherSmallerTgtHllType() throws Exception
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
    Assert.assertEquals(TGT_HLL_TYPE, result.getTgtHllType());
  }

  @Test
  public void testGetMergingFactoryOtherLargerTgtHllType() throws Exception
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
    Assert.assertEquals(largerTgtHllType, result.getTgtHllType());
  }

  @Test
  public void testGetMergingFactoryThisNoRoundOtherNoRound() throws Exception
  {
    HllSketchAggregatorFactory result = (HllSketchAggregatorFactory) targetNoRound.getMergingFactory(targetNoRound);
    Assert.assertFalse(result.isRound());
  }

  @Test
  public void testGetMergingFactoryThisNoRoundOtherRound() throws Exception
  {
    HllSketchAggregatorFactory result = (HllSketchAggregatorFactory) targetNoRound.getMergingFactory(targetRound);
    Assert.assertTrue(result.isRound());
  }

  @Test
  public void testGetMergingFactoryThisRoundOtherNoRound() throws Exception
  {
    HllSketchAggregatorFactory result = (HllSketchAggregatorFactory) targetRound.getMergingFactory(targetNoRound);
    Assert.assertTrue(result.isRound());
  }

  @Test
  public void testGetMergingFactoryThisRoundOtherRound() throws Exception
  {
    HllSketchAggregatorFactory result = (HllSketchAggregatorFactory) targetRound.getMergingFactory(targetRound);
    Assert.assertTrue(result.isRound());
  }
}
