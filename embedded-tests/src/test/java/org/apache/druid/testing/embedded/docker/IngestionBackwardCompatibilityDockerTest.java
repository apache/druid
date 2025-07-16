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

package org.apache.druid.testing.embedded.docker;

import org.apache.druid.testing.DruidContainer;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.junit.jupiter.api.Nested;

/**
 * Runs some basic ingestion tests using {@link DruidContainers} to verify
 * backward compatibility with old Druid images.
 */
public class IngestionBackwardCompatibilityDockerTest
{
  @Nested
  public class Apache31 extends IngestionDockerTest
  {
    @Override
    public EmbeddedDruidCluster createCluster()
    {
      coordinator.withImage(DruidContainer.Image.APACHE_31);
      overlordLeader.withImage(DruidContainer.Image.APACHE_31);
      return super.createCluster();
    }
  }
}
