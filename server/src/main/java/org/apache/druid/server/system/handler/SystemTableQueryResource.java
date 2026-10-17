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

package org.apache.druid.server.system.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.query.Query;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.QueryResourceQueryResultPusherFactory;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.ResourceIOReaderWriterFactory;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.AuthorizerMapper;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import java.io.IOException;
import java.io.InputStream;

/** Native query endpoint for nodes that serve only local system-table scans. */
@LazySingleton
@Path("/druid/v2/")
public class SystemTableQueryResource extends QueryResource
{
  @Inject
  public SystemTableQueryResource(
      final QueryLifecycleFactory queryLifecycleFactory,
      final @Json ObjectMapper jsonMapper,
      final QueryScheduler queryScheduler,
      final AuthorizerMapper authorizerMapper,
      final QueryResourceQueryResultPusherFactory queryResultPusherFactory,
      final ResourceIOReaderWriterFactory resourceIOReaderWriterFactory,
      final ServerConfig serverConfig
  )
  {
    super(
        queryLifecycleFactory,
        jsonMapper,
        queryScheduler,
        authorizerMapper,
        queryResultPusherFactory,
        resourceIOReaderWriterFactory,
        serverConfig
    );
  }

  @Override
  protected Query<?> readQuery(
      final HttpServletRequest req,
      final InputStream in,
      final ResourceIOReaderWriterFactory.ResourceIOReaderWriter ioReaderWriter
  ) throws IOException
  {
    final Query<?> query = super.readQuery(req, in, ioReaderWriter);
    if (!(query instanceof ScanQuery)
        || !(query.getDataSource() instanceof SystemTableDataSource)) {
      throw new BadQueryContextException(
          "This native query endpoint accepts only local system-table Scan queries"
      );
    }
    return query;
  }
}
