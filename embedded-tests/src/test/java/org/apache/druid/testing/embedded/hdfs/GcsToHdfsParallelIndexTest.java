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
import org.apache.druid.testing.embedded.gcs.AbstractGcsInputSourceParallelIndexTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

/**
 * Embedded parallel index test that reads input data from Google Cloud Storage (FakeGcsServer
 * testcontainer) and stores segments in HDFS (in-process MiniDFSCluster).
 *
 * <p>The HDFS resource is registered after the GCS resource so that the HDFS deep-storage
 * configuration ({@code druid.storage.type=hdfs}) overrides the GCS configuration set by the
 * FakeGcsServer resource. The GCS input-source connection properties remain active and are used
 * by the indexer to read data from FakeGcsServer.
 */
public class GcsToHdfsParallelIndexTest extends AbstractGcsInputSourceParallelIndexTest
{
  private final HdfsStorageResource hdfsResource = new HdfsStorageResource(true);

  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    // GCS/FakeGcsServer resource first: uploads data to FakeGcsServer and sets GCS connection
    // properties.
    super.addResources(cluster);
    // HDFS resource second: overrides druid.storage.type to "hdfs" for deep storage.
    cluster.addResource(hdfsResource);
  }

  @ParameterizedTest
  @MethodSource("resources")
  public void testGcsIndexData(Pair<String, List<?>> gcsInputSource) throws Exception
  {
    doTest(gcsInputSource, new Pair<>(false, false), "google", null);
  }
}
