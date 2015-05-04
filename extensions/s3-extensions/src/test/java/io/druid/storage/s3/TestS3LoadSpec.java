/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.storage.s3;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class TestS3LoadSpec
{
  private S3DataSegmentPuller puller = null;
  @Before
  public void setUp(){
    puller = new S3DataSegmentPuller(null);
  }
  @Test
  public void testToURI() throws URISyntaxException
  {
    S3LoadSpec loadSpec = new S3LoadSpec(puller,"bucket","key");
    Assert.assertEquals(new URI("s3://bucket/key"), loadSpec.getURI());
  }
}
