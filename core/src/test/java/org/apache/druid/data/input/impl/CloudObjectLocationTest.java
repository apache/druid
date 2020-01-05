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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;

public class CloudObjectLocationTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String SCHEME = "s3";
  private static final String BUCKET_NAME = "bucket";

  private static final CloudObjectLocation LOCATION =
      new CloudObjectLocation(BUCKET_NAME, "path/to/myobject");

  private static final CloudObjectLocation LOCATION_EXTRA_SLASHES =
      new CloudObjectLocation(BUCKET_NAME + '/', "/path/to/myobject");

  private static final CloudObjectLocation LOCATION_URLENCODE =
      new CloudObjectLocation(BUCKET_NAME, "path/to/myobject?question");

  private static final CloudObjectLocation LOCATION_NON_ASCII =
      new CloudObjectLocation(BUCKET_NAME, "pÄth/tø/myøbject");

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws Exception
  {
    Assert.assertEquals(
        LOCATION,
        MAPPER.readValue(MAPPER.writeValueAsString(LOCATION), CloudObjectLocation.class)
    );

    Assert.assertEquals(
        LOCATION_EXTRA_SLASHES,
        MAPPER.readValue(MAPPER.writeValueAsString(LOCATION_EXTRA_SLASHES), CloudObjectLocation.class)
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
        URI.create("s3://bucket/path/to/myobject"),
        LOCATION.toUri(SCHEME)
    );

    Assert.assertEquals(
        URI.create("s3://bucket/path/to/myobject"),
        LOCATION_EXTRA_SLASHES.toUri(SCHEME)
    );

    Assert.assertEquals(
        URI.create("s3://bucket/path/to/myobject%3Fquestion"),
        LOCATION_URLENCODE.toUri(SCHEME)
    );

    Assert.assertEquals(
        URI.create("s3://bucket/p%C3%84th/t%C3%B8/my%C3%B8bject"),
        LOCATION_NON_ASCII.toUri(SCHEME)
    );
  }

  @Test
  public void testUriRoundTrip()
  {
    Assert.assertEquals(LOCATION, new CloudObjectLocation(LOCATION.toUri(SCHEME)));
    Assert.assertEquals(LOCATION_EXTRA_SLASHES, new CloudObjectLocation(LOCATION_EXTRA_SLASHES.toUri(SCHEME)));
    Assert.assertEquals(LOCATION_URLENCODE, new CloudObjectLocation(LOCATION_URLENCODE.toUri(SCHEME)));
    Assert.assertEquals(LOCATION_NON_ASCII, new CloudObjectLocation(LOCATION_NON_ASCII.toUri(SCHEME)));
  }

  @Test
  public void testBucketName()
  {
    expectedException.expect(IllegalArgumentException.class);
    CloudObjectLocation invalidBucket = new CloudObjectLocation("someBÜcket", "some/path");
    // will never get here
    Assert.assertEquals(invalidBucket, new CloudObjectLocation(invalidBucket.toUri(SCHEME)));
  }
}
