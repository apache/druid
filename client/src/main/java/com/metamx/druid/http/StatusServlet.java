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

package com.metamx.druid.http;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

/**
 */
public class StatusServlet extends HttpServlet
{
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    ByteArrayOutputStream retVal = new ByteArrayOutputStream();
    PrintWriter out = new PrintWriter(new OutputStreamWriter(retVal));

    Runtime runtime = Runtime.getRuntime();
    long maxMemory = runtime.maxMemory();
    long totalMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();

    out.printf("Max Memory:\t%,18d\t%1$d%n", maxMemory);
    out.printf("Total Memory:\t%,18d\t%1$d%n", totalMemory);
    out.printf("Free Memory:\t%,18d\t%1$d%n", freeMemory);
    out.printf("Used Memory:\t%,18d\t%1$d%n", totalMemory - freeMemory);

    out.flush();

    resp.setStatus(HttpServletResponse.SC_OK);
    resp.setContentType("text/plain");
    resp.getOutputStream().write(retVal.toByteArray());
  }
}
