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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Embedded parallel index test that reads from and writes to HDFS using an in-process
 * {@link org.apache.hadoop.hdfs.MiniDFSCluster}.
 */
public class HdfsToHdfsParallelIndexTest extends AbstractHdfsInputSourceParallelIndexTest
{
  private final HdfsStorageResource hdfsResource = new HdfsStorageResource(true);

  @Override
  protected HdfsStorageResource getHdfsResource()
  {
    return hdfsResource;
  }

  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    cluster.addResource(hdfsResource);
  }

  @ParameterizedTest
  @MethodSource("resources")
  public void testHdfsIndexData(Pair<String, Object> hdfsInputSource) throws Exception
  {
    doHdfsTest(hdfsInputSource, new Pair<>(false, false));
  }
}
