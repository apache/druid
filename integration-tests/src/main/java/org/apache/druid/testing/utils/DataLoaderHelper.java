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

package org.apache.druid.testing.utils;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;

public final class DataLoaderHelper
{
  private static final Logger LOG = new Logger(SqlTestQueryHelper.class);
  @Inject
  private SqlTestQueryHelper sqlTestQueryHelper;

  @Inject
  private CoordinatorResourceTestClient coordinator;

  public void waitUntilDatasourceIsReady(String datasource)
  {
    LOG.info("Waiting for Segments to load for datasource [%s]", datasource);
    ITRetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(datasource),
        StringUtils.format(
            "Segment Load for datasource [%s]",
            datasource
        )
    );
    LOG.info("Segments loaded for datasource [%s]", datasource);

    LOG.info("Waiting for datasource [%s] to be ready for SQL queries", datasource);
    ITRetryUtil.retryUntilTrue(
        () -> sqlTestQueryHelper.isDatasourceLoadedInSQL(datasource),
        StringUtils.format("Waiting for [%s] to be ready for SQL queries", datasource)
    );
    LOG.info("Datasource [%s] ready for SQL queries", datasource);
  }
}
