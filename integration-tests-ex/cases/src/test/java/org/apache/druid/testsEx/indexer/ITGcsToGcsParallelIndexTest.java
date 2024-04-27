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

package org.apache.druid.testsEx.indexer;

import junitparams.Parameters;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testsEx.categories.GcsDeepStorage;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

/**
 * IMPORTANT:
 * To run this test, you must set the following env variables in the build environment -
 * GOOGLE_PREFIX - path inside the bucket where the test data files will be uploaded
 * GOOGLE_BUCKET - Google cloud bucket name
 * GOOGLE_APPLICATION_CREDENTIALS - path to the json file containing google cloud credentials
 * <a href="https://druid.apache.org/docs/latest/development/extensions-core/google.html">Google Cloud Storage setup in druid</a>
 */

@RunWith(DruidTestRunner.class)
@Category(GcsDeepStorage.class)
public class ITGcsToGcsParallelIndexTest extends AbstractGcsInputSourceParallelIndexTest
{
  @Test
  @Parameters(method = "resources")
  public void testGcsIndexData(Pair<String, List<?>> gcsInputSource) throws Exception
  {
    doTest(gcsInputSource, new Pair<>(false, false), "google");
  }
}
