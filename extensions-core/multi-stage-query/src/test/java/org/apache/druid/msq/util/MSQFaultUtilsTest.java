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

package org.apache.druid.msq.util;

import org.apache.druid.msq.indexing.error.MSQFaultUtils;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.error.WorkerFailedFault;
import org.junit.Assert;
import org.junit.Test;

public class MSQFaultUtilsTest
{


  @Test
  public void testGetErrorCodeFromMessage()
  {
    Assert.assertEquals(UnknownFault.CODE, MSQFaultUtils.getErrorCodeFromMessage(
        "Task execution process exited unsuccessfully with code[137]. See middleManager logs for more details..."));

    Assert.assertEquals(UnknownFault.CODE, MSQFaultUtils.getErrorCodeFromMessage(""));
    Assert.assertEquals(UnknownFault.CODE, MSQFaultUtils.getErrorCodeFromMessage(null));

    Assert.assertEquals("ABC", MSQFaultUtils.getErrorCodeFromMessage("ABC: xyz xyz : xyz"));

    Assert.assertEquals(
        WorkerFailedFault.CODE,
        MSQFaultUtils.getErrorCodeFromMessage(MSQFaultUtils.generateMessageWithErrorCode(new WorkerFailedFault(
            "123",
            "error"
        )))
    );
  }
}
