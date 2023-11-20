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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class S3DataSegmentPusherConfigTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerialization() throws IOException
  {
    String jsonConfig = "{\"bucket\":\"bucket1\",\"baseKey\":\"dataSource1\","
                        + "\"disableAcl\":false,\"maxListingLength\":2000,\"useS3aSchema\":false}";

    S3DataSegmentPusherConfig config = JSON_MAPPER.readValue(jsonConfig, S3DataSegmentPusherConfig.class);
    Map<String, String> expected = JSON_MAPPER.readValue(jsonConfig, Map.class);
    Map<String, String> actual = JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(config), Map.class);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSerializationWithDefaults() throws IOException
  {
    String jsonConfig = "{\"bucket\":\"bucket1\",\"baseKey\":\"dataSource1\"}";
    String expectedJsonConfig = "{\"bucket\":\"bucket1\",\"baseKey\":\"dataSource1\","
                                + "\"disableAcl\":false,\"maxListingLength\":1024,\"useS3aSchema\":false}";
    S3DataSegmentPusherConfig config = JSON_MAPPER.readValue(jsonConfig, S3DataSegmentPusherConfig.class);
    Map<String, String> expected = JSON_MAPPER.readValue(expectedJsonConfig, Map.class);
    Map<String, String> actual = JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(config), Map.class);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSerializationValidatingMaxListingLength() throws IOException
  {
    String jsonConfig = "{\"bucket\":\"bucket1\",\"baseKey\":\"dataSource1\","
                        + "\"disableAcl\":false,\"maxListingLength\":-1}";
    Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    S3DataSegmentPusherConfig config = JSON_MAPPER.readValue(jsonConfig, S3DataSegmentPusherConfig.class);
    Set<ConstraintViolation<S3DataSegmentPusherConfig>> violations = validator.validate(config);
    Assert.assertEquals(1, violations.size());
    ConstraintViolation violation = Iterators.getOnlyElement(violations.iterator());
    Assert.assertEquals("must be greater than or equal to 1", violation.getMessage());
  }
}
