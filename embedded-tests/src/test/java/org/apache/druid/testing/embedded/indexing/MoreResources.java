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

package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchMergeAggregatorFactory;

import java.util.function.Supplier;

/**
 * Additional utility methods used in embedded tests that are not added to
 * {@code Resources} to avoid cyclical dependencies.
 */
public class MoreResources
{
  /**
   * Task payload builders.
   */
  public static class Task
  {
    /**
     * A minimal 'index' task that ingests inline data {@link Resources.InlineData#CSV_10_DAYS}
     * using "DAY" granularity.
     */
    public static final Supplier<TaskBuilder.Index> BASIC_INDEX =
        () -> TaskBuilder
            .ofTypeIndex()
            .isoTimestampColumn("time")
            .csvInputFormatWithColumns("time", "item", "value")
            .inlineInputSourceWithData(Resources.InlineData.CSV_10_DAYS)
            .segmentGranularity("DAY")
            .dimensions();

    public static final Supplier<TaskBuilder.Index> INDEX_TASK_WITH_AGGREGATORS =
        () -> TaskBuilder
            .ofTypeIndex()
            .jsonInputFormat()
            .localInputSourceWithFiles(
                Resources.DataFile.tinyWiki1Json(),
                Resources.DataFile.tinyWiki2Json(),
                Resources.DataFile.tinyWiki3Json()
            )
            .timestampColumn("timestamp")
            .dimensions(
                "page",
                "language", "tags", "user", "unpatrolled", "newPage", "robot",
                "anonymous", "namespace", "continent", "country", "region", "city"
            )
            .metricAggregates(
                new CountAggregatorFactory("ingested_events"),
                new DoubleSumAggregatorFactory("added", "added"),
                new DoubleSumAggregatorFactory("deleted", "deleted"),
                new DoubleSumAggregatorFactory("delta", "delta"),
                new SketchMergeAggregatorFactory("thetaSketch", "user", null, null, null, null),
                new HllSketchBuildAggregatorFactory("HLLSketchBuild", "user", null, null, null, null, true),
                new DoublesSketchAggregatorFactory("quantilesDoublesSketch", "delta", null)
            )
            .dynamicPartitionWithMaxRows(3)
            .granularitySpec("DAY", "SECOND", true)
            .appendToExisting(false);
  }

  public static class MSQ
  {
    /**
     * SQL to INSERT any of the tiny wiki JSON data files into a new datasource
     * with DAY granularity. e.g. {@link Resources.DataFile#tinyWiki1Json()}.
     */
    public static final String INSERT_TINY_WIKI_JSON =
        "INSERT INTO %s\n"
        + "SELECT\n"
        + "  TIME_PARSE(\"timestamp\") AS __time,\n"
        + "  isRobot,\n"
        + "  diffUrl,\n"
        + "  added,\n"
        + "  countryIsoCode,\n"
        + "  regionName,\n"
        + "  channel,\n"
        + "  flags,\n"
        + "  delta,\n"
        + "  isUnpatrolled,\n"
        + "  isNew,\n"
        + "  deltaBucket,\n"
        + "  isMinor,\n"
        + "  isAnonymous,\n"
        + "  deleted,\n"
        + "  cityName,\n"
        + "  metroCode,\n"
        + "  namespace,\n"
        + "  comment,\n"
        + "  page,\n"
        + "  commentLength,\n"
        + "  countryName,\n"
        + "  user,\n"
        + "  regionIsoCode\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"local\",\"files\":[\"%s\"]}',\n"
        + "    '{\"type\":\"json\"}',\n"
        + "    "
        + "'[{\"type\":\"string\",\"name\":\"timestamp\"},"
        + "{\"type\":\"string\",\"name\":\"isRobot\"},"
        + "{\"type\":\"string\",\"name\":\"diffUrl\"},"
        + "{\"type\":\"long\",\"name\":\"added\"},"
        + "{\"type\":\"string\",\"name\":\"countryIsoCode\"},"
        + "{\"type\":\"string\",\"name\":\"regionName\"},"
        + "{\"type\":\"string\",\"name\":\"channel\"},"
        + "{\"type\":\"string\",\"name\":\"flags\"},"
        + "{\"type\":\"long\",\"name\":\"delta\"},"
        + "{\"type\":\"string\",\"name\":\"isUnpatrolled\"},"
        + "{\"type\":\"string\",\"name\":\"isNew\"},"
        + "{\"type\":\"double\",\"name\":\"deltaBucket\"},"
        + "{\"type\":\"string\",\"name\":\"isMinor\"},"
        + "{\"type\":\"string\",\"name\":\"isAnonymous\"},"
        + "{\"type\":\"long\",\"name\":\"deleted\"},"
        + "{\"type\":\"string\",\"name\":\"cityName\"},"
        + "{\"type\":\"long\",\"name\":\"metroCode\"},"
        + "{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},"
        + "{\"type\":\"string\",\"name\":\"page\"},"
        + "{\"type\":\"long\",\"name\":\"commentLength\"},"
        + "{\"type\":\"string\",\"name\":\"countryName\"},"
        + "{\"type\":\"string\",\"name\":\"user\"},"
        + "{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
        + "  )\n"
        + ")\n"
        + "PARTITIONED BY DAY\n";
  }

  public static class ProbufData
  {
    public static final String WIKI_PROTOBUF_BYTES_DECODER_RESOURCE = "data/protobuf/wikipedia.desc";
    public static final String WIKI_PROTO_MESSAGE_TYPE = "Wikipedia";
  }
}
