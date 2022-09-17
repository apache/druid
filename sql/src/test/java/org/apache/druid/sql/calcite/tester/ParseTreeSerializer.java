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
import org.apache.calcite.sql.SqlNode;

/**
 * Serializes the Calcite parse tree into form handy for the
 * test cases. Puts each node on a separate line.
 */
public class ParseTreeSerializer
{
  private int level;
  private String prefix;
  private final StringBuilder buf = new StringBuilder();

  public void indent()
  {
    for (int i = 0; i < level; i++) {
      buf.append("  ");
    }
  }

  public void prefix(String prefix)
  {
    this.prefix = prefix;
  }

  public void node(SqlNode node, String details)
  {
    indent();
    emitPrefix();
    String name = node.getClass().getSimpleName();
    if (name.startsWith("Sql")) {
      name = name.substring(3);
    }
    String kind = node.getKind().toString();
    buf.append(kind);
    if (!kind.equalsIgnoreCase(name)) {
      buf.append(" - ");
      buf.append(name);
    }
    if (!Strings.isNullOrEmpty(details)) {
      buf.append(" (");
      buf.append(details);
      buf.append(")");
    }
    buf.append("\n");
  }

  public void text(String text)
  {
    indent();
    emitPrefix();
    buf.append(text);
    buf.append("\n");
  }

  private void emitPrefix()
  {
    if (prefix != null) {
      buf.append(prefix);
      buf.append(": ");
      prefix = null;
    }
  }

  public void push()
  {
    level++;
  }

  public void pop()
  {
    level--;
  }

  public String result()
  {
    return buf.toString();
  }
}
