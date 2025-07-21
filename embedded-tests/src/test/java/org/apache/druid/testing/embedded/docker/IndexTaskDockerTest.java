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

import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.EmbeddedIndexTaskTest;
import org.apache.druid.testing.embedded.mariadb.MariaDBMetadataResource;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;

/**
 * Simulation tests for batch {@link IndexTask} using inline datasources.
 */
public class IndexTaskDockerTest extends EmbeddedIndexTaskTest
{
  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withZookeeper()
                               .useLatchableEmitter()
                               .addResource(new MariaDBMetadataResource())
                               .addResource(new MinIOStorageResource())
                               .addServer(coordinator)
                               .addServer(indexer)
                               .addServer(overlord)
                               .addServer(historical)
                               .addServer(broker)
                               .addServer(new EmbeddedRouter());
  }
}
