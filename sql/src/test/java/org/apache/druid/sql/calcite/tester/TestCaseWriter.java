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

package org.apache.druid.sql.calcite.tester;

import com.google.common.base.Strings;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.http.SqlParameter;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Writes (emits) the test case file format.
 */
public class TestCaseWriter
{
  private final Writer writer;

  public TestCaseWriter(Writer writer)
  {
    this.writer = writer;
  }

  public void emitCase(String label) throws IOException
  {
    emitSection("case", label);
  }

  public void emitComment(List<String> comment) throws IOException
  {
    writer.append("==============================================================\n");
    if (comment == null) {
      return;
    }
    emitLines(comment);
  }

  public void emitComment(String comment) throws IOException
  {
    writer.append("==============================================================\n");
    emitOptionalLine(comment);
  }

  public void emitCopy(String section) throws IOException
  {
    writer.append("=== ")
          .append(section)
          .append(" copy\n");
  }

  public void emitSql(String sql) throws IOException
  {
    emitSection("SQL", sql);
  }

  public void emitContext(Map<String, Object> context) throws IOException
  {
    emitMap("context", context);
  }

  public void emitOptions(Map<String, ?> options) throws IOException
  {
    emitMap("options", options);
  }

  private void emitMap(String section, Map<String, ?> map) throws IOException
  {
    if (map.isEmpty()) {
      return;
    }
    emitSection(section);
    List<String> keys = new ArrayList<>(map.keySet());
    Collections.sort(keys);
    for (String key : keys) {
      emitQuotedString(key);
      writer.append(": ");
      emitJsonValue(map.get(key));
      writer.append("\n");
    }
  }

  private void emitJsonValue(Object value) throws IOException
  {
    if (value == null) {
      writer.append("null");
    } else if (value instanceof String) {
      emitQuotedString((String) value);
    } else {
      writer.append(value.toString());
    }
  }

  private void emitQuotedString(String str) throws IOException
  {
    str = StringUtils.replace(str, "\\", "\\\\");
    str = StringUtils.replace(str, "\"", "\\\"");
    writer.append("\"")
          .append(str)
          .append("\"");
  }

  public void emitUser(String user) throws IOException
  {
    emitSection("user", user);
  }

  public void emitParameters(List<SqlParameter> parameters) throws IOException
  {
    emitSection("parameters");
    for (SqlParameter p : parameters) {
      emitQuotedString(p.getType().name());
      writer.append(", ");
      Object value = p.getValue();
      if (p.getValue() == null) {
        writer.append("null");
      } else {
        emitJsonValue(value);
      }
      writer.append("\n");
    }
  }

  public void emitException(Exception exception) throws IOException
  {
    emitSection("exception", exception.getClass().getSimpleName());
  }

  public void emitError(Exception exception) throws IOException
  {
    emitSection("error", exception.getMessage());
  }

  public void emitResources(Set<ResourceAction> resourceActions) throws IOException
  {
    emitSection("resources");

    // Sort resources so the output is deterministic. Some queries both
    // read and write the same datasource, so include the action in the sort.
    List<ResourceAction> actions = new ArrayList<>(resourceActions);
    Collections.sort(
        actions,
        (l, r) -> {
          int value = l.getResource().getType().compareTo(r.getResource().getType());
          if (value != 0) {
            return value;
          }
          value = l.getResource().getName().compareTo(r.getResource().getName());
          if (value != 0) {
            return value;
          }
          return l.getAction().compareTo(r.getAction());
        }
    );
    for (ResourceAction action : actions) {
      emitOptionalLine(new Resources.Resource(action).toString());
    }
  }

  public void emitResources(List<Resources.Resource> resources) throws IOException
  {
    emitSection("resources");

    for (Resources.Resource resource : resources) {
      writer
          .append(resource.type)
          .append("/")
          .append(resource.name)
          .append("/")
          .append(resource.action.name())
          .append("\n");
    }
  }

  void emitSchema(String[] schema) throws IOException
  {
    emitSection("schema", schema);
  }

  void emitNative(String nativeQuery) throws IOException
  {
    emitSection("native", nativeQuery);
  }

  void emitPlan(String plan) throws IOException
  {
    emitSection("plan", plan);
  }

  void emitResults(String[] schema) throws IOException
  {
    emitSection("results", schema);
  }

  void emitResults(List<String> schema) throws IOException
  {
    emitSection("results", schema);
  }

  void emitSection(String section, String[] body) throws IOException
  {
    if (body == null) {
      return;
    }
    emitSection(section);
    for (String line : body) {
      emitOptionalLine(line);
    }
  }

  void emitSection(String section, List<String> body) throws IOException
  {
    if (body == null) {
      return;
    }
    emitSection(section);
    emitLines(body);
  }

  public void emitSection(String section, String body) throws IOException
  {
    if (body == null) {
      return;
    }
    emitSection(section);
    emitOptionalLine(body);
  }

  public void emitSection(String section) throws IOException
  {
    writer.append("=== ")
          .append(section)
          .append("\n");
  }

  public void emitLines(List<String> lines) throws IOException
  {
    for (String line : lines) {
      emitLine(line);
    }
  }

  public void emitOptionalLine(String line) throws IOException
  {
    if (!Strings.isNullOrEmpty(line)) {
      emitLine(line);
    }
  }

  public void emitLine(String line) throws IOException
  {
    writer.append(line);
    if (!line.endsWith("\n")) {
      writer.append('\n');
    }
  }

  public void emitPattern(String line) throws IOException
  {
    writer.append("!");
    emitOptionalLine(line);
  }

  public void emitLiteral(String line) throws IOException
  {
    if (line.length() == 0) {
      emitLine(line);
      return;
    }
    char c = line.charAt(0);
    if ("\\=*!".indexOf(c) != -1) {
      writer.append("\\");
      return;
    }
    emitLine(line);
  }

  public void emitErrors(List<String> errors) throws IOException
  {
    if (errors.isEmpty()) {
      return;
    }
    // Errors emitted as a comment.
    emitOptionalLine("==== Verification Errors ====");
    emitLines(errors);
  }
}
