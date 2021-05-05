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

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HllSketchMergeAggregatorFactoryTest
{
  private static final String NAME = "name";
  private static final String FIELD_NAME = "fieldName";
  private static final int LG_K = 2;
  private static final String TGT_HLL_TYPE = TgtHllType.HLL_6.name();
  private static final StringEncoding STRING_ENCODING = StringEncoding.UTF16LE;
  private static final boolean ROUND = true;

  private HllSketchMergeAggregatorFactory targetRound;
  private HllSketchMergeAggregatorFactory targetNoRound;

  @Before
  public void setUp()
  {
    targetRound = new HllSketchMergeAggregatorFactory(NAME, FIELD_NAME, LG_K, TGT_HLL_TYPE, STRING_ENCODING, ROUND);
    targetNoRound = new HllSketchMergeAggregatorFactory(NAME, FIELD_NAME, LG_K, TGT_HLL_TYPE, STRING_ENCODING, !ROUND);
  }

  @Test(expected = AggregatorFactoryNotMergeableException.class)
  public void testGetMergingFactoryBadName() throws Exception
  {
    HllSketchMergeAggregatorFactory other = new HllSketchMergeAggregatorFactory(
        NAME + "-diff",
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        STRING_ENCODING,
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
        STRING_ENCODING,
        ROUND
    );
    targetRound.getMergingFactory(other);
  }

  @Test(expected = AggregatorFactoryNotMergeableException.class)
  public void testGetMergingFactoryDifferentStringEncoding() throws Exception
  {
    HllSketchMergeAggregatorFactory other = new HllSketchMergeAggregatorFactory(
        NAME,
        FIELD_NAME,
        LG_K,
        TGT_HLL_TYPE,
        StringEncoding.UTF8,
        ROUND
    );
    HllSketchAggregatorFactory result = (HllSketchAggregatorFactory) targetRound.getMergingFactory(other);
    Assert.assertEquals(LG_K, result.getLgK());
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
        STRING_ENCODING,
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
        STRING_ENCODING,
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
        STRING_ENCODING,
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
        STRING_ENCODING,
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

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper().copy();
    jsonMapper.registerModules(new HllSketchModule().getJacksonModules());

    final HllSketchMergeAggregatorFactory factory = new HllSketchMergeAggregatorFactory(
        "foo",
        "bar",
        18,
        TgtHllType.HLL_8.name(),
        StringEncoding.UTF8,
        true
    );

    final String serializedString = jsonMapper.writeValueAsString(factory);

    Assert.assertEquals(
        "{\"type\":\"HLLSketchMerge\",\"name\":\"foo\",\"fieldName\":\"bar\",\"lgK\":18,\"tgtHllType\":\"HLL_8\",\"stringEncoding\":\"utf8\",\"round\":true}",
        serializedString
    );

    final AggregatorFactory factory2 = jsonMapper.readValue(
        serializedString,
        AggregatorFactory.class
    );

    Assert.assertEquals(factory, factory2);
  }

  @Test
  public void testSerdeWithDefaults() throws IOException
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper().copy();
    jsonMapper.registerModules(new HllSketchModule().getJacksonModules());

    final HllSketchMergeAggregatorFactory factory = new HllSketchMergeAggregatorFactory(
        "foo",
        "bar",
        null,
        null,
        null,
        false
    );

    final String serializedString = jsonMapper.writeValueAsString(factory);

    Assert.assertEquals(
        "{\"type\":\"HLLSketchMerge\",\"name\":\"foo\",\"fieldName\":\"bar\",\"lgK\":12,\"tgtHllType\":\"HLL_4\"}",
        serializedString
    );

    final AggregatorFactory factory2 = jsonMapper.readValue(
        serializedString,
        AggregatorFactory.class
    );

    Assert.assertEquals(factory, factory2);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(HllSketchBuildAggregatorFactory.class).usingGetClass().verify();
  }
}
