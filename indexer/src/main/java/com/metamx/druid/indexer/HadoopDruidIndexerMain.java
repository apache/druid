/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.indexer;

import com.google.common.collect.ImmutableList;
import com.metamx.common.Pair;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;

import java.util.List;

/**
 */
public class HadoopDruidIndexerMain
{
  private static final Logger log = new Logger(HadoopDruidIndexerMain.class);

  public static void main(String[] args) throws Exception
  {
    if (args.length != 1) {
      printHelp();
      System.exit(2);
    }

    HadoopDruidIndexerNode node = HadoopDruidIndexerNode.builder().build();

    node.setArgumentSpec(args[0]);

    Lifecycle lifecycle = new Lifecycle();
    lifecycle.addManagedInstance(node);

    try {
      lifecycle.start();
    }
    catch (Throwable t) {
      log.info(t, "Throwable caught at startup, committing seppuku");
      Thread.sleep(500);
      printHelp();
      System.exit(1);
    }
  }

  private static final List<Pair<String, String>> expectedFields =
      ImmutableList.<Pair<String, String>>builder()
                   .add(Pair.of("dataSource", "Name of dataSource"))
                   .add(Pair.of("timestampColumn", "Column name of the timestamp column"))
                   .add(Pair.of("timestampFormat", "Format name of the timestamp column (posix or iso)"))
                   .add(
                       Pair.of(
                           "dataSpec",
                           "A JSON object with fields "
                           +
                           "format=(json, csv, tsv), "
                           +
                           "columns=JSON array of column names for the delimited text input file (only for csv and tsv formats),"
                           +
                           "dimensions=JSON array of dimensionn names (must match names in columns),"
                           +
                           "delimiter=delimiter of the data (only for tsv format)"
                       )
                   )
                   .add(Pair.of("granularitySpec", "A JSON object indicating the Granularity that segments should be created at."))
                   .add(
                       Pair.of(
                           "pathSpec",
                           "A JSON object with fields type=granularity, inputPath, filePattern, dataGranularity"
                       )
                   )
                   .add(
                       Pair.of(
                           "rollupSpec",
                           "JSON object with fields rollupGranularity, aggs=JSON Array of Aggregator specs"
                       )
                   )
                   .add(Pair.of("workingPath", "Path to store intermediate output data.  Deleted when finished."))
                   .add(Pair.of("segmentOutputPath", "Path to store output segments."))
                   .add(
                       Pair.of(
                           "updaterJobSpec",
                           "JSON object with fields type=db, connectURI of the database, username, password, and segment table name"
                       )
                   )
                   .add(Pair.of("cleanupOnFailure", "Clean up intermediate files on failure? (default: true)"))
                   .add(Pair.of("leaveIntermediate", "Leave intermediate files. (default: false)"))
                   .add(Pair.of("partitionDimension", "Dimension to partition by (optional)"))
                   .add(
                       Pair.of(
                           "targetPartitionSize",
                           "Integer representing the target number of rows in a partition (required if partitionDimension != null)"
                       )
                   )
                   .build();

  public static void printHelp()
  {
    System.out.println("Usage: <java invocation> <config_spec>");
    System.out.println("<config_spec> is either a JSON object or the path to a file that contains a JSON object.");
    System.out.println();
    System.out.println("JSON object description:");
    System.out.println("{");
    for (Pair<String, String> expectedField : expectedFields) {
      System.out.printf("  \"%s\": %s%n", expectedField.lhs, expectedField.rhs);
    }
    System.out.println("}");
  }
}
