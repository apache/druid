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

package org.apache.druid.testing.embedded.hdfs;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.azure.AbstractAzureInputSourceParallelIndexTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

/**
 * Embedded parallel index test that reads input data from Azure Blob Storage (Azurite
 * testcontainer) and stores segments in HDFS (in-process MiniDFSCluster).
 *
 * <p>The HDFS resource is registered after the Azure resource so that the HDFS deep-storage
 * configuration ({@code druid.storage.type=hdfs}) overrides the Azure configuration set by
 * the Azurite resource. The Azure input-source connection properties remain active and are used
 * by the indexer to read data from Azurite.
 */
public class AzureToHdfsParallelIndexTest extends AbstractAzureInputSourceParallelIndexTest
{
  private final HdfsStorageResource hdfsResource = new HdfsStorageResource(true);

  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    // Azure/Azurite resource first: uploads data to Azurite and sets Azure connection properties.
    super.addResources(cluster);
    // HDFS resource second: overrides druid.storage.type to "hdfs" for deep storage.
    cluster.addResource(hdfsResource);
  }

  @ParameterizedTest
  @MethodSource("resources")
  public void testAzureIndexData(Pair<String, List<?>> azureInputSource) throws Exception
  {
    doTest(azureInputSource, new Pair<>(false, false), "azure", null);
  }
}
