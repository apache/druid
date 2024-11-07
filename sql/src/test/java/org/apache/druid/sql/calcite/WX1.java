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

package org.apache.druid.sql.calcite;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class WX1
{
  public static void main(String[] args) throws IOException
  {

    String pathname = "./src/test/java/org/apache/druid/sql/calcite/CalciteTableAppendTest.java";
    // pathname=CalciteSubqueryTest.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    System.out.println(pathname);
    Path path = new File(pathname).toPath();
    List<String> lines = Files.readAllLines(path);
    List<String> newLines = new ArrayList<String>();

    String st = null;
    String colLine = null;
    for (String l : lines) {

      if (l.contains("ScanQueryBuilder")) {
        st = l;
      }
      if (l.contains("columns(") && l.contains(")")) {
        colLine = l;
      }
      if (l.contains("columnTypes")) {
        colLine = null;
      }
      if (l.contains(";")) {
        st = null;
        colLine = null;
      }

      if (colLine != null && l != colLine && !l.contains("columnTypes")) {
        System.out.println(colLine);
        String[] p0 = colLine.split("[()]");
        String[] aa = p0[1].split(",");

        StringBuffer sb = new StringBuffer();

        sb.append(colLine.substring(0, colLine.indexOf("c")));
        sb.append("columnTypes(");

        for (String colName : aa) {
          colName = StringUtils.strip(colName);
          colName = colName.replaceAll("\"", "");

          String type = getTypeForColName(colName);

          sb.append(type + ", ");
        }
        int ll = sb.length();
        sb.delete(ll - 2, ll);
        sb.append(")");
        System.out.println(l);
        System.out.println(sb.toString());
        colLine = null;
        newLines.add(sb.toString());
      }

      newLines.add(l);
    }

    Files.write(path, newLines);

  }

  private static String getTypeForColName(String colName)
  {
    switch (colName)
    {
      case "f1":
      case "m1":
        return "ColumnType.FLOAT";
      case "d1":
      case "m2":
        return "ColumnType.DOUBLE";
      case "unique_dim1":
        return "ColumnType.ofComplex(\"hyperUnique\")";
      case "cnt":
      case "__time":
        return "ColumnType.LONG";
      default:
        return "ColumnType.STRING";
    }
  }
}
