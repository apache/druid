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
import org.apache.druid.testing.embedded.indexer.AbstractS3InputSourceParallelIndexTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

/**
 * Embedded parallel index test that reads input data from S3 (MinIO testcontainer) and stores
 * segments in HDFS (in-process MiniDFSCluster).
 *
 * <p>The HDFS resource is registered after the MinIO resource so that the HDFS deep-storage
 * configuration ({@code druid.storage.type=hdfs}) overrides the S3 configuration set by MinIO.
 * The S3 input-source connection properties remain active and are used by the indexer to read
 * data from MinIO.
 */
public class S3ToHdfsParallelIndexTest extends AbstractS3InputSourceParallelIndexTest
{
  private final HdfsStorageResource hdfsResource = new HdfsStorageResource(true);

  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    // S3/MinIO resource first: uploads data to MinIO and sets S3 connection properties.
    super.addResources(cluster);
    // HDFS resource second: overrides druid.storage.type to "hdfs" for deep storage.
    cluster.addResource(hdfsResource);
  }

  @ParameterizedTest
  @MethodSource("resources")
  public void testS3IndexData(Pair<String, List<?>> s3InputSource) throws Exception
  {
    doTest(s3InputSource, new Pair<>(false, false), "s3", null);
  }
}
