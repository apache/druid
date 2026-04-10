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

package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.error.DruidException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public final class MetricAllowlistLoader
{

  public static Set<String> loadAllowlistFromFile(
      final ObjectMapper mapper,
      final String allowlistPath,
      final MetricAllowlistParser parser
  )
  {
    validateAllowlistPath(allowlistPath);

    final File allowlistFile = new File(allowlistPath);

    try (final InputStream is = new FileInputStream(allowlistFile)) {
      return parseAllowlist(mapper, is, allowlistFile.getPath(), parser);
    }
    catch (FileNotFoundException e) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.NOT_FOUND)
                          .build("Metric spec file path [%s] was not found.", allowlistFile.getPath());
    }
    catch (IOException e) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build("Failed to parse metric spec file path [%s].", allowlistFile.getPath());
    }
  }

  public static Set<String> loadAllowlistFromClasspath(
      final ObjectMapper mapper,
      final String resourcePath,
      final MetricAllowlistParser parser
  )
  {
    validateAllowlistPath(resourcePath);

    final InputStream classpathInputStream = MetricAllowlistLoader.class.getClassLoader().getResourceAsStream(resourcePath);
    if (classpathInputStream == null) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.NOT_FOUND)
                          .build("Metric spec file path [%s] was not found.", resourcePath);
    }

    try (final InputStream is = classpathInputStream) {
      return parseAllowlist(mapper, is, resourcePath, parser);
    }
    catch (IOException e) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build("Failed to parse metric spec file path [%s].", resourcePath);
    }
  }

  private static Set<String> parseAllowlist(
      final ObjectMapper mapper,
      final InputStream inputStream,
      final String source,
      final MetricAllowlistParser parser
  ) throws IOException
  {
    return parser.parse(mapper.readTree(inputStream), source);
  }

  private static void validateAllowlistPath(final String allowlistPath)
  {
    if (Strings.isNullOrEmpty(allowlistPath)) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build("Metric spec file path was empty, value [%s].", allowlistPath);
    }
  }
}
