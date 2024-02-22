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

import org.junit.Test;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

public class A11
{

  static class MyException extends Exception
  {

    private static final long serialVersionUID = 1L;
    private static final String SS = "at proc.mounts.case(proc.mounts:10)";

    MyException()
    {

    }

    public void printStackTrace(PrintWriter s)
    {
      s.println(this);
      for (StackTraceElement se : fakeStackElements()) {
        s.println("\tat " + se);
      }
      s.println(SS);
      super.printStackTrace(s);
    }

    public void printStackTrace(PrintStream s)
    {
      s.println(this);
      for (StackTraceElement se : fakeStackElements()) {
        s.println("\tat " + se);
      }
      s.println(SS);
      super.printStackTrace(s);
    }

    public StackTraceElement[] getStackTrace()
    {
      StackTraceElement[] stackTrace = super.getStackTrace();

      List<StackTraceElement> trace = Arrays.asList(stackTrace);
      StackTraceElement[] aa = fakeStackElements();
      for (StackTraceElement stackTraceElement : aa) {
        trace.add(stackTraceElement);
      }

      return trace.toArray(new StackTraceElement[] {});
    }

    private StackTraceElement[] fakeStackElements()
    {
      StackTraceElement aa = super.getStackTrace()[0];
      StackTraceElement fake = new StackTraceElement(
          "app1",
          "proc.mounts",
          null,
          "A11",
          "bar",
          "processing/src/test/resources/proc.mounts",
          10);

      return new StackTraceElement[] {fake};
    }

  }


  private static final String FRAME_PREFIX = "at ";

  @Test
  public void ttt() throws A11.MyException, Exception
  {
    System.err.println(getClass().getResource("/proc.mounts")+":10");
    System.err.println(getClass().getResource("/proc.mounts")+":10");
    System.err.println("asdsad (https://asd.hu/sd)");
    System.err.println(new URL("http://asd.hu/asd"));
    System.err.println("at xx(A11.java:10)");
    System.err.println("at xx(A12.java:10)");

//    try {
//    A11.main(null);
//    }catch(Exception e) {
//      e.printStackTrace();
//      throw e;
//    }
  }

  public static void main(String[] args) throws A11.MyException
  {

    extracted();
  }

  private static void extracted() throws A11.MyException
  {
    throw new MyException();
  }


  @Test
  public void ss () {
    String traceLine = "at proc.mounts.case(proc.mounts:10)";
    String testName= traceLine;
    int indexOfFramePrefix= testName.indexOf(FRAME_PREFIX);
    if (indexOfFramePrefix == -1) {
            return ;
    }
    testName= testName.substring(indexOfFramePrefix);
    testName= testName.substring(FRAME_PREFIX.length(), testName.lastIndexOf('(')).trim();
    int indexOfModuleSeparator= testName.lastIndexOf('/');
    if (indexOfModuleSeparator != -1) {
            testName= testName.substring(indexOfModuleSeparator + 1);
    }
    testName= testName.substring(0, testName.lastIndexOf('.'));
    int innerSeparatorIndex= testName.indexOf('$');
    if (innerSeparatorIndex != -1) {
      testName= testName.substring(0, innerSeparatorIndex);
    }

    String lineNumber= traceLine;
    lineNumber= lineNumber.substring(lineNumber.indexOf(':') + 1, lineNumber.lastIndexOf(')'));
    int line= Integer.parseInt(lineNumber);
    newOpenEditorAtLineAction( testName, line);


  }

  private Object newOpenEditorAtLineAction(String testName, int line)
  {
    System.out.println(testName);
    System.out.println(line);
    return line;

  }

}
