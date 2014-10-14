/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import io.druid.jackson.DefaultObjectMapper;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class HadoopDruidIndexerConfigTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  public static <T> T jsonReadWriteRead(String s, Class<T> klass)
  {
    try {
      return jsonMapper.readValue(jsonMapper.writeValueAsBytes(jsonMapper.readValue(s, klass)), klass);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Test
  public void shouldMakeHDFSCompliantSegmentOutputPath()
  {
    HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{\n"
          + "    \"dataSchema\": {\n"
          + "        \"dataSource\": \"source\",\n"
          + "        \"metricsSpec\": [],\n"
          + "        \"granularitySpec\": {\n"
          + "            \"type\": \"uniform\",\n"
          + "            \"segmentGranularity\": \"hour\",\n"
          + "            \"intervals\": [\"2012-07-10/P1D\"]\n"
          + "        }\n"
          + "    },\n"
          + "    \"ioConfig\": {\n"
          + "        \"type\": \"hadoop\",\n"
          + "        \"segmentOutputPath\": \"hdfs://server:9100/tmp/druid/datatest\"\n"
          + "    }\n"
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    HadoopDruidIndexerConfig cfg = new HadoopDruidIndexerConfig(
        schema.withTuningConfig(
            schema.getTuningConfig()
                  .withVersion(
                      "some:brand:new:version"
                  )
        )
    );

    Bucket bucket = new Bucket(4711, new DateTime(2012, 07, 10, 5, 30), 4712);
    Path path = cfg.makeSegmentOutputPath(new DistributedFileSystem(), bucket);
    Assert.assertEquals(
        "hdfs://server:9100/tmp/druid/datatest/source/20120710T050000.000Z_20120710T060000.000Z/some_brand_new_version/4712",
        path.toString()
    );
  }

  @Test
  public void shouldMakeDefaultSegmentOutputPathIfNotHDFS()
  {
    final HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{\n"
          + "    \"dataSchema\": {\n"
          + "        \"dataSource\": \"the:data:source\",\n"
          + "        \"metricsSpec\": [],\n"
          + "        \"granularitySpec\": {\n"
          + "            \"type\": \"uniform\",\n"
          + "            \"segmentGranularity\": \"hour\",\n"
          + "            \"intervals\": [\"2012-07-10/P1D\"]\n"
          + "        }\n"
          + "    },\n"
          + "    \"ioConfig\": {\n"
          + "        \"type\": \"hadoop\",\n"
          + "        \"segmentOutputPath\": \"/tmp/dru:id/data:test\"\n"
          + "    }\n"
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    HadoopDruidIndexerConfig cfg = new HadoopDruidIndexerConfig(
        schema.withTuningConfig(
            schema.getTuningConfig()
                  .withVersion(
                      "some:brand:new:version"
                  )
        )
    );

    Bucket bucket = new Bucket(4711, new DateTime(2012, 07, 10, 5, 30), 4712);
    Path path = cfg.makeSegmentOutputPath(new LocalFileSystem(), bucket);
    Assert.assertEquals(
        "/tmp/dru:id/data:test/the:data:source/2012-07-10T05:00:00.000Z_2012-07-10T06:00:00.000Z/some:brand:new:version/4712",
        path.toString()
    );

  }
}
