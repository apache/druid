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

package org.apache.druid.indexing.seekablestream;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class TaskConfigUpdateRequestTest
{
  @Test
  public void testTaskConfigUpdateRequest()
  {
    SeekableStreamIndexTaskIOConfig mockIoConfig = EasyMock.createMock(SeekableStreamIndexTaskIOConfig.class);
    EasyMock.replay(mockIoConfig);

    TaskConfigUpdateRequest request = new TaskConfigUpdateRequest(mockIoConfig);
    Assert.assertEquals(mockIoConfig, request.getIoConfig());

    TaskConfigUpdateRequest nullRequest = new TaskConfigUpdateRequest(null);
    Assert.assertNull(nullRequest.getIoConfig());

    TaskConfigUpdateRequest request2 = new TaskConfigUpdateRequest(mockIoConfig);
    Assert.assertEquals(request, request2);
    Assert.assertEquals(request.hashCode(), request2.hashCode());

    Assert.assertNotEquals(request, nullRequest);
    Assert.assertNotEquals(request.hashCode(), nullRequest.hashCode());

    String toString = request.toString();
    Assert.assertTrue(toString.contains("TaskConfigUpdateRequest"));
    Assert.assertTrue(toString.contains("ioConfig"));

    EasyMock.verify(mockIoConfig);
  }
}
