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

package io.druid.cli.convert;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexing.common.task.HadoopIndexTask;
import io.druid.indexing.common.task.IndexTask;
import io.druid.indexing.common.task.RealtimeIndexTask;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.realtime.FireDepartment;

import java.io.File;

/**
 */
@Command(
    name = "convertSpec",
    description = "Converts the old Druid ingestion spec to the new version"
)
public class ConvertIngestionSpec implements Runnable
{
  @Option(name = "-o", title = "old ingestion file", description = "file with old ingestion spec", required = true)
  public String oldFile;

  @Option(name = "-n", title = "new ingestion file", description = "file with new ingestion spec", required = true)
  public String newFile;

  @Option(name = "-t", title = "type", description = "the type of ingestion spec to convert. types[standalone_realtime, cli_hadoop, index_realtime, index_hadoop, index]", required = true)
  public String type;

  @Override
  public void run()
  {
    File file = new File(oldFile);
    if (!file.exists()) {
      System.out.printf("File[%s] does not exist.%n", file);
    }

    final ObjectMapper jsonMapper = new DefaultObjectMapper();

    try {
      String converterType = jsonMapper.writeValueAsString(ImmutableMap.of("type", type));
      IngestionSchemaConverter val = jsonMapper.readValue(converterType, IngestionSchemaConverter.class);
      jsonMapper.writerWithDefaultPrettyPrinter().writeValue(new File(newFile), val.convert(jsonMapper, file));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "standalone_realtime", value = StandaloneRealtimeIngestionSchemaConverter.class),
      @JsonSubTypes.Type(name = "cli_hadoop", value = CliHadoopIngestionSchemaConverter.class),
      @JsonSubTypes.Type(name = "index_realtime", value = IndexRealtimeIngestionSchemaConverter.class),
      @JsonSubTypes.Type(name = "index_hadoop", value = IndexHadoopIngestionSchemaConverter.class),
      @JsonSubTypes.Type(name = "index", value = IndexIngestionSchemaConverter.class),
  })
  private static interface IngestionSchemaConverter<T>
  {
    public T convert(ObjectMapper jsonMapper, File oldFile) throws Exception;
  }

  private static class StandaloneRealtimeIngestionSchemaConverter implements IngestionSchemaConverter<FireDepartment>
  {
    @Override
    public FireDepartment convert(ObjectMapper jsonMapper, File oldFile) throws Exception
    {
      return jsonMapper.readValue(oldFile, FireDepartment.class);
    }
  }

  private static class CliHadoopIngestionSchemaConverter implements IngestionSchemaConverter<HadoopDruidIndexerConfig>
  {
    @Override
    public HadoopDruidIndexerConfig convert(ObjectMapper jsonMapper, File oldFile) throws Exception
    {
      return new HadoopDruidIndexerConfig(
          jsonMapper.readValue(oldFile, HadoopIngestionSpec.class),
          null
      );
    }
  }

  private static class IndexRealtimeIngestionSchemaConverter implements IngestionSchemaConverter<RealtimeIndexTask>
  {
    @Override
    public RealtimeIndexTask convert(ObjectMapper jsonMapper, File oldFile) throws Exception
    {
      return jsonMapper.readValue(oldFile, RealtimeIndexTask.class);
    }
  }

  private static class IndexHadoopIngestionSchemaConverter implements IngestionSchemaConverter<HadoopIndexTask>
  {
    @Override
    public HadoopIndexTask convert(ObjectMapper jsonMapper, File oldFile) throws Exception
    {
      return jsonMapper.readValue(oldFile, HadoopIndexTask.class);
    }
  }

  private static class IndexIngestionSchemaConverter implements IngestionSchemaConverter<IndexTask>
  {
    @Override
    public IndexTask convert(ObjectMapper jsonMapper, File oldFile) throws Exception
    {
      return jsonMapper.readValue(oldFile, IndexTask.class);
    }
  }
}
