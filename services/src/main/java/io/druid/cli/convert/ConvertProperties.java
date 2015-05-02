/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.cli.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Sets;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.druid.jackson.DefaultObjectMapper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

/**
 */
@Command(
    name = "convertProps",
    description = "Converts runtime.properties files from version to version"
)
public class ConvertProperties implements Runnable
{
  private static final Logger log = new Logger(ConvertProperties.class);

  private static String parseJdbcUrl(String jdbcUrl){
    final String subString = jdbcUrl.substring("jdbc:".length());
    final URI uri = URI.create(subString);
    return uri.getScheme();
  }

  private static final List<PropertyConverter> converters6to7 = ImmutableList.<PropertyConverter>of(
      new Rename("druid.db.connector.connectURI", "druid.metadata.storage.connector.connectURI"),
      new Rename("druid.db.connector.user", "druid.metadata.storage.connector.user"),
      new Rename("druid.db.connector.password", "druid.metadata.storage.connector.password"),
      new Remove("druid.db.connector.validationQuery"),
      new Remove("druid.db.connector.useValidationQuery"),
      new Rename("druid.db.connector.createTables", "druid.metadata.storage.connector.createTables"),
      new Rename("druid.db.tables.base", "druid.metadata.storage.tables.base"),
      new Rename("druid.db.tables.configTable", "druid.metadata.storage.tables.configTable"),
      new Rename("druid.db.tables.segmentTable", "druid.metadata.storage.tables.segmentTable"),
      new Rename("druid.db.tables.ruleTable", "druid.metadata.storage.tables.ruleTable"),
      new Rename("druid.db.tables.taskLock", "druid.metadata.storage.tables.taskLock"),
      new Rename("druid.db.tables.tasks", "druid.metadata.storage.tables.tasks"),
      new Rename("druid.db.tables.taskLog", "druid.metadata.storage.tables.taskLog"),
      new PropertyConverter()
      {
        // Add a new config for metadata storage type, and update property name
        private static final String PROPERTY = "druid.db.connector.connectURI";

        @Override
        public boolean canHandle(String property)
        {
          return PROPERTY.equals(property);
        }

        @Override
        public Map<String, String> convert(Properties properties)
        {
          if (properties.containsKey(PROPERTY)) {
            String jdbcConnectorType = parseJdbcUrl(properties.getProperty(PROPERTY));
            return ImmutableMap.of(
                "druid.metadata.storage.connector.connectURI", properties.getProperty(PROPERTY),
                "druid.metadata.storage.type", jdbcConnectorType
            );
          } else {
            return ImmutableMap.of();
          }
        }
      },
      new PropertyConverter()
      {
        // Add a new coordinate for the metadata storage
        private static final String PROPERTY = "druid.extensions.coordinates";
        private final ObjectMapper defaultObjectMapper = new DefaultObjectMapper();
        @Override
        public boolean canHandle(String property)
        {
          return PROPERTY.equals(property);
        }

        private static final String uriPropertyKey = "druid.db.connector.connectURI";
        @Override
        public Map<String, String> convert(Properties properties)
        {
          final String jdbcUrl = properties.getProperty(uriPropertyKey);
          if(null == jdbcUrl){
            log.warn("No entry for [%s] found in properties! cannot add ????-metadata-storage to [%s]", uriPropertyKey, PROPERTY);
            return ImmutableMap.of();
          }

          final String value = properties.getProperty(PROPERTY);
          final Set<String> coordinates = Sets.newHashSet();
          final List<String> oldCoordinates;
          try {
            oldCoordinates = defaultObjectMapper.readValue(
                value, new TypeReference<List<String>>(){}
            );
          }
          catch (IOException e) {
            throw Throwables.propagate(e);
          }
          coordinates.addAll(oldCoordinates);
          coordinates.add(String.format("io.druid.extensions:%s-metadata-storage", parseJdbcUrl(jdbcUrl)));
          try {
            return ImmutableMap.of(PROPERTY, defaultObjectMapper.writeValueAsString(ImmutableList.copyOf(coordinates)));
          }
          catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
          }
        }
      },
      new ValueConverter("druid.indexer.storage.type", ImmutableMap.of("db", "metadata")),
      new ValueConverter("druid.publish.type", ImmutableMap.of("db", "metadata"))
  );

  @Option(name = "-f", title = "file", description = "The properties file to convert", required = true)
  public String filename;

  @Option(name = "-o", title = "outFile", description = "The file to write updated properties to.", required = true)
  public String outFilename;

  @Override
  public void run()
  {
    File file = new File(filename);
    if (!file.exists()) {
      System.out.printf("File[%s] does not exist.%n", file);
    }

    File outFile = new File(outFilename);
    if (outFile.getParentFile() != null && !outFile.getParentFile().exists()) {
      outFile.getParentFile().mkdirs();
    }

    Properties fromFile = new Properties();

    try (Reader in = new InputStreamReader(new FileInputStream(file), Charsets.UTF_8)) {
      fromFile.load(in);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    Properties updatedProps = new Properties();

    int count = 0;
    for (String property : fromFile.stringPropertyNames()) {
      boolean handled = false;
      for (PropertyConverter converter : converters6to7) {
        if (converter.canHandle(property)) {
          for (Map.Entry<String, String> entry : converter.convert(fromFile).entrySet()) {
            if (entry.getValue() != null) {
              ++count;
              log.info("Converting [%s] to [%s]:[%s]", property, entry.getKey(), entry.getValue());
              updatedProps.setProperty(entry.getKey(), entry.getValue());
            }
          }
          handled = true;
        }
      }

      if (!handled) {
        log.info("Not converting [%s]", property);
        updatedProps.put(property, fromFile.getProperty(property));
      }
    }

    BufferedWriter out = null;
    try {
      TreeMap<Object, Object> orderedUpdates = new TreeMap<>();
      orderedUpdates.putAll(updatedProps);
      out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile), Charsets.UTF_8));
      for (Map.Entry<Object, Object> prop : orderedUpdates.entrySet()) {
        out.write((String) prop.getKey());
        out.write("=");
        out.write((String) prop.getValue());
        out.newLine();
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    finally {
      if (out != null) {
        CloseQuietly.close(out);
      }
    }

    log.info("Completed!  Converted[%,d] properties.", count);
  }
}
