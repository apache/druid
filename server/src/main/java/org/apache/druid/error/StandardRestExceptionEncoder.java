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

package org.apache.druid.error;

import org.apache.druid.java.util.common.logger.Logger;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class StandardRestExceptionEncoder implements RestExceptionEncoder
{
  private static final RestExceptionEncoder INSTANCE = new StandardRestExceptionEncoder();
  private static final Logger LOG = new Logger(StandardRestExceptionEncoder.class);

  private final Properties catalog;

  public static RestExceptionEncoder instance()
  {
    return INSTANCE;
  }

  public StandardRestExceptionEncoder()
  {
    // Load the default error catalog, if it exists.
    this.catalog = new Properties();
    File catalogFile = new File("conf/druid/errors.properties");
    if (catalogFile.isFile()) {
      try (Reader reader = new BufferedReader(
          new InputStreamReader(
              new FileInputStream(catalogFile),
              StandardCharsets.UTF_8))) {
        this.catalog.load(reader);
        LOG.info(
            "Loaded [%d] entries from error catalog file [%s]",
            catalog.size(),
            catalogFile.getAbsolutePath()
        );
      }
      catch (IOException e) {
        // Warn about failures, but don't take the server down. We'll run
        // with standard errors.
        LOG.error(e, "Failed to load error catalog file [%s]", catalogFile.getAbsolutePath());
      }
    }
  }

  @Override
  public ResponseBuilder builder(DruidException e)
  {
    return Response
      .status(Response.Status.fromStatusCode(e.httpStatus()))
      .entity(e.toErrorResponse(catalog))
      .type(MediaType.APPLICATION_JSON);
  }

  @Override
  public Response encode(DruidException e)
  {
    return builder(e).build();
  }
}
