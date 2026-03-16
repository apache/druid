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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Embedded parallel index test that reads input data from HDFS (in-process MiniDFSCluster)
 * and stores segments in S3 (MinIO testcontainer).
 *
 * <p>The MinIO resource is registered after the HDFS resource so that the S3 deep-storage
 * configuration ({@code druid.storage.type=s3}) is applied last. The HDFS input-source
 * connection properties ({@code hadoop.fs.defaultFS}) remain active and are used by the
 * indexer to read data from HDFS.
 */
public class HdfsToS3ParallelIndexTest extends AbstractHdfsInputSourceParallelIndexTest
{
  private static final Logger LOG = new Logger(HdfsToS3ParallelIndexTest.class);

  /** HDFS is the input source only — deep storage is S3/MinIO. */
  private final HdfsStorageResource hdfsResource = new HdfsStorageResource(false);
  private final MinIOStorageResource minIOResource = new MinIOStorageResource();

  @Override
  protected HdfsStorageResource getHdfsResource()
  {
    return hdfsResource;
  }

  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    // HDFS resource: starts the MiniDFSCluster and sets hadoop.fs.defaultFS so the indexer
    // can read from HDFS. configureAsDeepStorage=false means it does NOT set druid.storage.type.
    cluster.addResource(hdfsResource);

    // MinIO resource: configures S3/MinIO as deep storage (druid.storage.type=s3, etc.).
    // Adding it after the HDFS resource ensures the S3 deep-storage settings win.
    cluster.addResource(minIOResource);
  }

  @ParameterizedTest
  @MethodSource("resources")
  public void testHdfsIndexData(Pair<String, Object> hdfsInputSource) throws Exception
  {
    doHdfsTest(hdfsInputSource, new Pair<>(false, false));
  }
}
