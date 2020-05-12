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

package org.apache.druid.storage.s3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class S3InputDataConfigTest
{
  private static final int MAX_LISTING_LENGTH_TOO_LOW = S3InputDataConfig.MAX_LISTING_LENGTH_MIN - 1;
  private static final int MAX_LISTING_LENGTH_TOO_HIGH = S3InputDataConfig.MAX_LISTING_LENGTH_MAX + 1;
  private static final String INPUT_DATA_TEMPLATE_JSON_STR =
      "{\n"
      + "    \"maxListingLength\": \"%1$d\"\n"
      + "}";
  private static final String INPUT_DATA_MAX_LISTING_LENGTH_TOO_LOW_JSON_STR =
      StringUtils.format(INPUT_DATA_TEMPLATE_JSON_STR, MAX_LISTING_LENGTH_TOO_LOW);

  private static final String INPUT_DATA_MAX_LISTING_LENGTH_TOO_HIGH_JSON_STR =
      StringUtils.format(INPUT_DATA_TEMPLATE_JSON_STR, MAX_LISTING_LENGTH_TOO_HIGH);

  private static final String INPUT_DATA_MAX_LISTING_LENGTH_MIN_JSON_STR =
      StringUtils.format(INPUT_DATA_TEMPLATE_JSON_STR, S3InputDataConfig.MAX_LISTING_LENGTH_MIN);

  private static final String INPUT_DATA_MAX_LISTING_LENGTH_MAX_JSON_STR =
      StringUtils.format(INPUT_DATA_TEMPLATE_JSON_STR, S3InputDataConfig.MAX_LISTING_LENGTH_MAX);

  public static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private S3InputDataConfig inputDataConfig;

  @Test
  public void test_construct_maxListingLengthTooLow_throwsException()
  {
    boolean exceptionThrown = false;
    try {
      inputDataConfig = JSON_MAPPER.readValue(INPUT_DATA_MAX_LISTING_LENGTH_TOO_LOW_JSON_STR, S3InputDataConfig.class);
    }
    catch (JsonProcessingException e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  @Test
  public void test_construct_maxListingLengthTooHigh_throwsException()
  {
    boolean exceptionThrown = false;
    try {
      inputDataConfig = JSON_MAPPER.readValue(INPUT_DATA_MAX_LISTING_LENGTH_TOO_HIGH_JSON_STR, S3InputDataConfig.class);
    }
    catch (JsonProcessingException e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  @Test
  public void test_construct_maxListingLengthMin_succeeds() throws JsonProcessingException
  {
    inputDataConfig = JSON_MAPPER.readValue(INPUT_DATA_MAX_LISTING_LENGTH_MIN_JSON_STR, S3InputDataConfig.class);
    Assert.assertEquals(S3InputDataConfig.MAX_LISTING_LENGTH_MIN, inputDataConfig.getMaxListingLength());
  }

  @Test
  public void test_construct_maxListingLengthMax_succeeds() throws JsonProcessingException
  {
    inputDataConfig = JSON_MAPPER.readValue(INPUT_DATA_MAX_LISTING_LENGTH_MAX_JSON_STR, S3InputDataConfig.class);
    Assert.assertEquals(S3InputDataConfig.MAX_LISTING_LENGTH_MAX, inputDataConfig.getMaxListingLength());
  }

  @Test
  public void test_setMaxListingLength_maxListingLengthTooLow_throwsException()
  {
    boolean exceptionThrown = false;
    try {
      inputDataConfig = new S3InputDataConfig();
      inputDataConfig.setMaxListingLength(MAX_LISTING_LENGTH_TOO_LOW);
    }
    catch (IAE e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  @Test
  public void test_setMaxListingLength_maxListingLengthTooHigh_throwsException()
  {
    boolean exceptionThrown = false;
    try {
      inputDataConfig = new S3InputDataConfig();
      inputDataConfig.setMaxListingLength(MAX_LISTING_LENGTH_TOO_HIGH);
    }
    catch (IAE e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  @Test
  public void test_setMaxListingLength_maxListingLengthMin_succeeds() throws IAE
  {
    inputDataConfig = new S3InputDataConfig();
    inputDataConfig.setMaxListingLength(S3InputDataConfig.MAX_LISTING_LENGTH_MIN);
    Assert.assertEquals(S3InputDataConfig.MAX_LISTING_LENGTH_MIN, inputDataConfig.getMaxListingLength());
  }

  @Test
  public void test_setMaxListingLength_maxListingLengthMax_succeeds() throws IAE
  {
    inputDataConfig = new S3InputDataConfig();
    inputDataConfig.setMaxListingLength(S3InputDataConfig.MAX_LISTING_LENGTH_MAX);
    Assert.assertEquals(S3InputDataConfig.MAX_LISTING_LENGTH_MAX, inputDataConfig.getMaxListingLength());
  }

  @Test
  public void test_construct_maxListingLengthDefaultsToMax()
  {
    inputDataConfig = new S3InputDataConfig();
    Assert.assertEquals(S3InputDataConfig.MAX_LISTING_LENGTH_MAX, inputDataConfig.getMaxListingLength());
  }
}
