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

package org.apache.druid.testing.simulate.junit5;

import org.apache.druid.testing.simulate.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.simulate.junit5.EmbeddedDruidClusterExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * JUnit 5 test that uses an {@link EmbeddedDruidCluster}.
 *
 * @see EmbeddedDruidClusterExtension for usage instructions
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DruidClusterTest
{
  protected EmbeddedDruidCluster cluster;

  protected abstract EmbeddedDruidCluster setupCluster();

  @BeforeAll
  protected void setup() throws Exception
  {
    cluster = setupCluster();
    cluster.before();
  }

  @AfterAll
  protected void tearDown()
  {
    if (cluster != null) {
      cluster.after();
    }
  }
}
