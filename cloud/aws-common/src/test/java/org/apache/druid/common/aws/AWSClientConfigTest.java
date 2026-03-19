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

package org.apache.druid.common.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AWSClientConfigTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testDefaultCrossRegionAccessEnabled() throws Exception
  {
    AWSClientConfig config = MAPPER.readValue("{}", AWSClientConfig.class);
    Assertions.assertNull(config.isForceGlobalBucketAccessEnabled());
    Assertions.assertFalse(config.isCrossRegionAccessEnabled());
  }

  @Test
  public void testCrossRegionAccessEnabledExplicitlySet() throws Exception
  {
    AWSClientConfig config = MAPPER.readValue("{\"crossRegionAccessEnabled\": true}", AWSClientConfig.class);
    Assertions.assertNull(config.isForceGlobalBucketAccessEnabled());
    Assertions.assertTrue(config.isCrossRegionAccessEnabled());
  }

  @Test
  public void testNewConfigTakesPrecedenceOverDeprecatedWhenBothSet() throws Exception
  {
    AWSClientConfig config = MAPPER.readValue(
        "{\"forceGlobalBucketAccessEnabled\": true, \"crossRegionAccessEnabled\": false}",
        AWSClientConfig.class
    );
    Assertions.assertFalse(config.isCrossRegionAccessEnabled());
  }

  @Test
  public void testNewConfigTrueWinsOverDeprecatedFalse() throws Exception
  {
    AWSClientConfig config = MAPPER.readValue(
        "{\"forceGlobalBucketAccessEnabled\": false, \"crossRegionAccessEnabled\": true}",
        AWSClientConfig.class
    );
    Assertions.assertTrue(config.isCrossRegionAccessEnabled());
  }

  @Test
  public void testDeprecatedForceGlobalBucketAccessAloneTrue() throws Exception
  {
    AWSClientConfig config = MAPPER.readValue(
        "{\"forceGlobalBucketAccessEnabled\": true}",
        AWSClientConfig.class
    );
    Assertions.assertTrue(config.isCrossRegionAccessEnabled());
  }

  @Test
  public void testDeprecatedNotSetFallsThroughToCrossRegion() throws Exception
  {
    AWSClientConfig config = MAPPER.readValue(
        "{\"crossRegionAccessEnabled\": true}",
        AWSClientConfig.class
    );
    Assertions.assertNull(config.isForceGlobalBucketAccessEnabled());
    Assertions.assertTrue(config.isCrossRegionAccessEnabled());
  }
}
