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

package io.druid.storage.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.io.IOException;
import java.util.Set;

public class S3DataSegmentPusherConfigTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testSerialization() throws IOException
  {
    String jsonConfig = "{\"bucket\":\"bucket1\",\"baseKey\":\"dataSource1\","
                        +"\"disableAcl\":false,\"maxListingLength\":2000,\"useS3aSchema\":false}";

    S3DataSegmentPusherConfig config = jsonMapper.readValue(jsonConfig, S3DataSegmentPusherConfig.class);
    Assert.assertEquals(jsonConfig, jsonMapper.writeValueAsString(config));
  }

  @Test
  public void testSerializationWithDefaults() throws IOException
  {
    String jsonConfig = "{\"bucket\":\"bucket1\",\"baseKey\":\"dataSource1\"}";
    String expectedJsonConfig = "{\"bucket\":\"bucket1\",\"baseKey\":\"dataSource1\","
                                +"\"disableAcl\":false,\"maxListingLength\":1000,\"useS3aSchema\":false}";

    S3DataSegmentPusherConfig config = jsonMapper.readValue(jsonConfig, S3DataSegmentPusherConfig.class);
    Assert.assertEquals(expectedJsonConfig, jsonMapper.writeValueAsString(config));
  }

  @Test
  public void testSerializationValidatingMaxListingLength() throws IOException
  {
    String jsonConfig = "{\"bucket\":\"bucket1\",\"baseKey\":\"dataSource1\","
                        +"\"disableAcl\":false,\"maxListingLength\":-1}";
    Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    S3DataSegmentPusherConfig config = jsonMapper.readValue(jsonConfig, S3DataSegmentPusherConfig.class);
    Set<ConstraintViolation<S3DataSegmentPusherConfig>> violations = validator.validate(config);
    Assert.assertEquals(1, violations.size());
    ConstraintViolation violation = Iterators.getOnlyElement(violations.iterator());
    Assert.assertEquals("must be greater than or equal to 0", violation.getMessage());
  }
}
