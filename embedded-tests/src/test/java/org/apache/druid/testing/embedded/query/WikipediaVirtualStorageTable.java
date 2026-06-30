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

package org.apache.druid.testing.embedded.query;

import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.shaded.com.google.common.io.ByteStreams;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

/**
 * Shared wikipedia ingest for the virtual-storage query tests. Loads the bundled {@code wikiticker} sample via MSQ,
 * partitioned by hour and clustered by channel, so the resulting datasource has many small segments with several
 * columns each - enough for partial loads to have something to coalesce.
 */
public final class WikipediaVirtualStorageTable
{
  private WikipediaVirtualStorageTable()
  {
  }

  /**
   * Ingest the sample into {@code dataSource} and return the task report so callers can make their own assertions
   * (e.g. segment count). Fails if the ingest task does not succeed.
   */
  public static MSQTaskReportPayload ingestHourly(
      EmbeddedDruidCluster cluster,
      EmbeddedMSQApis msqApis,
      EmbeddedBroker broker,
      String dataSource
  ) throws IOException
  {
    final File wikiFile = new File(cluster.getTestFolder().newFolder(), "wiki.gz");
    ByteStreams.copy(
        DruidProcessingConfigTest.class.getResourceAsStream("/wikipedia/wikiticker-2015-09-12-sampled.json.gz"),
        Files.newOutputStream(wikiFile.toPath())
    );
    final String sql = StringUtils.format(
        "SET waitUntilSegmentsLoad = TRUE;\n"
        + "REPLACE INTO \"%s\" OVERWRITE ALL\n"
        + "SELECT\n"
        + "  TIME_PARSE(\"time\") AS __time,\n"
        + "  channel,\n"
        + "  countryName,\n"
        + "  page,\n"
        + "  \"user\",\n"
        + "  added,\n"
        + "  deleted,\n"
        + "  delta\n"
        + "FROM TABLE(\n"
        + "    EXTERN(\n"
        + "      %s,\n"
        + "      '{\"type\":\"json\"}',\n"
        + "      '[{\"name\":\"isRobot\",\"type\":\"string\"},{\"name\":\"channel\",\"type\":\"string\"},{\"name\":\"time\",\"type\":\"string\"},{\"name\":\"flags\",\"type\":\"string\"},{\"name\":\"isUnpatrolled\",\"type\":\"string\"},{\"name\":\"page\",\"type\":\"string\"},{\"name\":\"diffUrl\",\"type\":\"string\"},{\"name\":\"added\",\"type\":\"long\"},{\"name\":\"comment\",\"type\":\"string\"},{\"name\":\"commentLength\",\"type\":\"long\"},{\"name\":\"isNew\",\"type\":\"string\"},{\"name\":\"isMinor\",\"type\":\"string\"},{\"name\":\"delta\",\"type\":\"long\"},{\"name\":\"isAnonymous\",\"type\":\"string\"},{\"name\":\"user\",\"type\":\"string\"},{\"name\":\"deltaBucket\",\"type\":\"long\"},{\"name\":\"deleted\",\"type\":\"long\"},{\"name\":\"namespace\",\"type\":\"string\"},{\"name\":\"cityName\",\"type\":\"string\"},{\"name\":\"countryName\",\"type\":\"string\"},{\"name\":\"regionIsoCode\",\"type\":\"string\"},{\"name\":\"metroCode\",\"type\":\"long\"},{\"name\":\"countryIsoCode\",\"type\":\"string\"},{\"name\":\"regionName\",\"type\":\"string\"}]'\n"
        + "    )\n"
        + "  )\n"
        + "PARTITIONED BY HOUR\n"
        + "CLUSTERED BY channel",
        dataSource,
        Calcites.escapeStringLiteral(
            broker.bindings()
                  .jsonMapper()
                  .writeValueAsString(new LocalInputSource(null, null, Collections.singletonList(wikiFile), null))
        )
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSqlAndGetReport(sql);
    Assertions.assertEquals(TaskState.SUCCESS, payload.getStatus().getStatus());
    Assertions.assertNull(payload.getStatus().getErrorReport());
    return payload;
  }
}
