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
