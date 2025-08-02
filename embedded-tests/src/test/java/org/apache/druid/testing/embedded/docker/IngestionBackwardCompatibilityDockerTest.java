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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.coordinator.CoordinatorServiceClient;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.indexing.SegmentUpdateResponse;
import org.apache.druid.testing.DruidContainer;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Nested;

/**
 * Runs some basic ingestion tests using {@code DruidContainers} to verify
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
      coordinator.usingImage(DruidContainer.Image.APACHE_31);
      overlordLeader.usingImage(DruidContainer.Image.APACHE_31);
      return super.createCluster();
    }

    @Override
    protected int markSegmentsAsUnused(String dataSource)
    {
      // For old Druid versions, use Coordinator to mark segments as unused
      final CoordinatorServiceClient coordinatorClient =
          overlordFollower.bindings().getInstance(CoordinatorServiceClient.class);

      try {
        RequestBuilder req = new RequestBuilder(
            HttpMethod.DELETE,
            StringUtils.format("/druid/coordinator/v1/datasources/%s", dataSource)
        );
        BytesFullResponseHolder responseHolder = coordinatorClient.getServiceClient().request(
            req,
            new BytesFullResponseHandler()
        );

        final ObjectMapper mapper = overlordFollower.bindings().jsonMapper();
        return mapper.readValue(responseHolder.getContent(), SegmentUpdateResponse.class).getNumChangedSegments();
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
