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

package org.apache.druid.storage.azure;

import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AzureCloudBlobHolderToCloudObjectLocationConverterTest extends EasyMockSupport
{
  private static final String CONTAINER1 = "container1";
  private static final String BLOB1 = "blob1";

  private CloudBlobHolder cloudBlob;

  private AzureCloudBlobHolderToCloudObjectLocationConverter converter;

  @Before
  public void setup()
  {
    cloudBlob = createMock(CloudBlobHolder.class);
  }

  @Test
  public void test_createCloudObjectLocation_returnsExpectedLocation() throws Exception
  {
    EasyMock.expect(cloudBlob.getContainerName()).andReturn(CONTAINER1);
    EasyMock.expect(cloudBlob.getName()).andReturn(BLOB1);
    replayAll();

    CloudObjectLocation expectedLocation = new CloudObjectLocation(CONTAINER1, BLOB1);
    converter = new AzureCloudBlobHolderToCloudObjectLocationConverter();
    CloudObjectLocation actualLocation = converter.createCloudObjectLocation(cloudBlob);

    Assert.assertEquals(expectedLocation, actualLocation);
    verifyAll();
  }
}
