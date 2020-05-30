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

package org.apache.druid.metadata;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TimingCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLMetadataTimingCollector implements TimingCollector
{
  static Logger logger = LoggerFactory.getLogger(SQLMetadataTimingCollector.class);

  private final ServiceEmitter emitter;
  private final MetadataStorageConnectorConfig config;

  @Inject
  public SQLMetadataTimingCollector(
      Supplier<MetadataStorageConnectorConfig> config,
      ServiceEmitter emitter)
  {
    this.config = config.get();
    this.emitter = emitter;
  }

  @Override
  public void collect(long elapsedTime, StatementContext ctx)
  {
    try {
      ServiceMetricEvent.Builder metricBuilder = ServiceMetricEvent.builder();
      metricBuilder.setDimension("uri", config.getConnectURI());

      // JDBI v2 does not provide the parsed SQL nor does the java.sql.PreparedStatement interface
      // so here we can only get the sql without any parameters from JDBI interface.
      // JDBI v3 provides StatementContext.getParsedSql to get the executed sql
      metricBuilder.setDimension("sql", ctx.getRewrittenSql());
      emitter.emit(metricBuilder.build("metadataStorage/sql/time", elapsedTime));
    }
    catch (Exception e) {
      logger.error("error to emit metadataStorage/sql/time", e);
    }
  }
}
