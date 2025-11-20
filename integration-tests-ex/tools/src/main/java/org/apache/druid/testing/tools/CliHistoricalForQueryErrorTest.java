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

package org.apache.druid.testing.tools;

import com.github.rvesse.airline.annotations.Command;
import com.google.inject.Binder;
import com.google.inject.Inject;
import org.apache.druid.cli.CliHistorical;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QuerySegmentWalker;

import java.util.Properties;

@Command(
    name = "historical-for-query-error-test",
    description = "Runs a Historical node modified for query error test"
)
public class CliHistoricalForQueryErrorTest extends CliHistorical
{
  private static final Logger log = new Logger(CliHistoricalForQueryErrorTest.class);

  public CliHistoricalForQueryErrorTest()
  {
    super();
  }

  @Inject
  @Override
  public void configure(Properties properties)
  {
    log.info("Historical is configured for testing query error on missing segments");
  }

  @Override
  public void bindQuerySegmentWalker(Binder binder)
  {
    binder.bind(QuerySegmentWalker.class).to(ServerManagerForQueryErrorTest.class).in(LazySingleton.class);
  }
}
