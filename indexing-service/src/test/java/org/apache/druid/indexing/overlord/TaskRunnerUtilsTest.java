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

package org.apache.druid.indexing.overlord;

import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;

public class TaskRunnerUtilsTest
{
  @Test
  public void testMakeWorkerURL()
  {
    final URL url = TaskRunnerUtils.makeWorkerURL(
        new Worker("https", "1.2.3.4:8290", "1.2.3.4", 1, "0", WorkerConfig.DEFAULT_CATEGORY),
        "/druid/worker/v1/task/%s/log",
        "foo bar&"
    );
    Assert.assertEquals("https://1.2.3.4:8290/druid/worker/v1/task/foo%20bar%26/log", url.toString());
    Assert.assertEquals("1.2.3.4:8290", url.getAuthority());
    Assert.assertEquals("/druid/worker/v1/task/foo%20bar%26/log", url.getPath());
  }
}
