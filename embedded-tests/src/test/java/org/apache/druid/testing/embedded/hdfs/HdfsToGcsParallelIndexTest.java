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

import com.google.cloud.storage.Storage;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.gcs.GoogleCloudStorageResource;
import org.apache.druid.testing.embedded.gcs.GoogleStorageTestModule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Embedded parallel index test that reads input data from HDFS (in-process MiniDFSCluster)
 * and stores segments in Google Cloud Storage (FakeGcsServer testcontainer).
 */
public class HdfsToGcsParallelIndexTest extends AbstractHdfsInputSourceParallelIndexTest
{
  private static final Logger LOG = new Logger(HdfsToGcsParallelIndexTest.class);

  /** HDFS is the input source only — deep storage is GCS. */
  private final HdfsStorageResource hdfsResource = new HdfsStorageResource(false);
  private final GoogleCloudStorageResource gcsResource = new GoogleCloudStorageResource();

  @Override
  protected HdfsStorageResource getHdfsResource()
  {
    return hdfsResource;
  }

  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    // HDFS resource: starts the MiniDFSCluster and sets hadoop.fs.defaultFS.
    // Does NOT configure HDFS as deep storage — GCS fills that role.
    cluster.addResource(hdfsResource);

    // GCS resource: configures GCS as deep storage (druid.storage.type=google, etc.)
    // and creates the GCS bucket. GoogleCloudStorageResource.onStarted() sets the
    // storageUrl from the running FakeGcsServer container.
    cluster.addResource(gcsResource);
  }

  @AfterAll
  public void deleteSegmentsFromGcs()
  {
    try (Storage storage = GoogleStorageTestModule.createStorageForTests(gcsResource.getUrl())) {
      // Delete all blobs under the deep-storage prefix for this test run.
      storage.list(gcsResource.getBucket()).iterateAll()
             .forEach(blob -> blob.delete());
    }
    catch (Exception e) {
      LOG.warn(e, "Unable to delete GCS blobs after test");
    }
  }

  @ParameterizedTest
  @MethodSource("resources")
  public void testHdfsIndexData(Pair<String, Object> hdfsInputSource) throws Exception
  {
    doHdfsTest(hdfsInputSource, new Pair<>(false, false));
  }
}
