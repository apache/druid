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

package org.apache.druid.testing.embedded.s3;

import org.apache.druid.data.input.s3.S3InputSourceDruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.console.WebConsoleTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.util.Map;

/**
 * Runs the web console specs that load data from S3, on a cluster with an S3-compatible container (which is also its
 * deep storage and where it keeps the task logs).
 */
public class S3WebConsoleTest extends WebConsoleTestBase
{
  private static final String DATA_FILE = "wikiticker-2015-09-12-sampled.json.gz";
  private static final String DATA_KEY = "web-console-data/" + DATA_FILE;

  private final S3StorageResource s3 = new S3StorageResource();

  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    cluster.addResource(s3);
  }

  @Override
  protected void configureCluster(EmbeddedDruidCluster cluster)
  {
    cluster.addExtension(S3InputSourceDruidModule.class);
  }

  @BeforeAll
  public void uploadData() throws Exception
  {
    final File dataFile = new File(webConsoleDir(), "../examples/quickstart/tutorial/" + DATA_FILE);
    s3.getS3Client().putObject(
        PutObjectRequest.builder().bucket(s3.getBucket()).key(DATA_KEY).build(),
        RequestBody.fromFile(dataFile)
    );
  }

  @Test
  public void testS3Ingestion() throws Exception
  {
    runSpec(
        "s3-ingestion.spec.ts",
        Map.of("DRUID_E2E_TEST_S3_URI", StringUtils.format("s3://%s/%s", s3.getBucket(), DATA_KEY))
    );
  }
}
