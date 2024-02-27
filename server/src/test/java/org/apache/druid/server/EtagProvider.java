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

package org.apache.druid.server;

import com.google.inject.Key;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public interface EtagProvider
{
  Key<EtagProvider> KEY = Key.get(EtagProvider.class);

  String getEtagFor(Query<?> query);

  class EmptyEtagProvider implements EtagProvider
  {
    @Override
    public String getEtagFor(Query<?> query)
    {
      return null;
    }
  }

  class ProvideEtagBasedOnDatasource implements EtagProvider
  {
    @Override
    public String getEtagFor(Query<?> query)
    {
      if (!query.getDataSource().isCacheable(true)) {
        return null;
      }
      if (!(query.getDataSource() instanceof TableDataSource)) {
        throw DruidException.defensive("only TableDataSource handled right now");
      }
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        TableDataSource tableDataSource = (TableDataSource) query.getDataSource();
        baos.write(query.getDataSource().getCacheKey());
        baos.write(tableDataSource.getName().getBytes(StandardCharsets.UTF_8));
        return "ETP-" + new String(baos.toByteArray(), StandardCharsets.UTF_8);
      }
      catch (IOException e) {
        throw DruidException.defensive().build(e, "Unexpected IOException");
      }
    }
  }
}
