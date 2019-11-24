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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;

public class CloudObjectLocationTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String SCHEME = "s3";

  private static final CloudObjectLocation LOCATION =
      new CloudObjectLocation("someBucket", "path/to/myobject");

  private static final CloudObjectLocation LOCATION_URLENCODE =
      new CloudObjectLocation("someBucket", "path/to/myobject?question");

  private static final CloudObjectLocation LOCATION_NON_ASCII =
      new CloudObjectLocation("someBucket", "pÄth/tø/myøbject");

  @Test
  public void testSerde() throws Exception
  {
    Assert.assertEquals(
        LOCATION,
        MAPPER.readValue(MAPPER.writeValueAsString(LOCATION), CloudObjectLocation.class)
    );

    Assert.assertEquals(
        LOCATION_URLENCODE,
        MAPPER.readValue(MAPPER.writeValueAsString(LOCATION_URLENCODE), CloudObjectLocation.class)
    );

    Assert.assertEquals(
        LOCATION_NON_ASCII,
        MAPPER.readValue(MAPPER.writeValueAsString(LOCATION_NON_ASCII), CloudObjectLocation.class)
    );
  }

  @Test
  public void testToUri()
  {
    Assert.assertEquals(
        URI.create("s3://someBucket/path/to/myobject"),
        LOCATION.toUri(SCHEME)
    );

    Assert.assertEquals(
        URI.create("s3://someBucket/path/to/myobject%3Fquestion"),
        LOCATION_URLENCODE.toUri(SCHEME)
    );

    Assert.assertEquals(
        URI.create("s3://someBucket/p%C3%84th/t%C3%B8/my%C3%B8bject"),
        LOCATION_NON_ASCII.toUri(SCHEME)
    );
  }

  @Test
  public void testUriRoundTrip()
  {
    Assert.assertEquals(LOCATION, new CloudObjectLocation(LOCATION.toUri(SCHEME)));
    Assert.assertEquals(LOCATION_URLENCODE, new CloudObjectLocation(LOCATION_URLENCODE.toUri(SCHEME)));
    Assert.assertEquals(LOCATION_NON_ASCII, new CloudObjectLocation(LOCATION_NON_ASCII.toUri(SCHEME)));
  }
}
