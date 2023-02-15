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

package org.apache.druid.k8s.overlord.common;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LogWatchInputStreamTest
{

  @Test
  void testFlow() throws IOException
  {
    LogWatch logWatch = mock(LogWatch.class);
    InputStream inputStream = mock(InputStream.class);
    when(inputStream.read()).thenReturn(1);
    when(logWatch.getOutput()).thenReturn(inputStream);
    KubernetesClient client = mock(KubernetesClient.class);
    LogWatchInputStream stream = new LogWatchInputStream(client, logWatch);
    int result = stream.read();
    Assertions.assertEquals(1, result);
    verify(inputStream, times(1)).read();
    stream.close();
    verify(logWatch, times(1)).close();
    verify(client, times(1)).close();
  }
}
