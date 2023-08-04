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

package org.apache.druid.storage.remote;

import com.google.common.base.Predicates;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class ChunkingStorageConnectorParametersTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ChunkingStorageConnectorParameters.class)
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testIncorrectParameters()
  {
    ChunkingStorageConnectorParameters.Builder<Void> builder = new ChunkingStorageConnectorParameters.Builder<>();
    builder.start(-1);
    Assert.assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  public void testCorrectParameters()
  {
    ChunkingStorageConnectorParameters.Builder<Void> builder = new ChunkingStorageConnectorParameters.Builder<>();
    builder.start(0);
    builder.end(10);
    builder.objectSupplier((start, end) -> null);
    builder.objectOpenFunction(obj -> null);
    builder.maxRetry(10);
    builder.cloudStoragePath("/path");
    builder.retryCondition(Predicates.alwaysTrue());
    builder.tempDirSupplier(() -> new File("/tmp"));
    ChunkingStorageConnectorParameters<Void> parameters = builder.build();
    Assert.assertEquals(0, parameters.getStart());
    Assert.assertEquals(10, parameters.getEnd());
    Assert.assertEquals(10, parameters.getMaxRetry());
    Assert.assertEquals("/path", parameters.getCloudStoragePath());
    Assert.assertEquals("/tmp", parameters.getTempDirSupplier().get().getAbsolutePath());
  }
}
