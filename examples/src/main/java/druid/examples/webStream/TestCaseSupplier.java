package druid.examples.webStream;

import com.google.common.collect.Lists;
import com.google.common.io.InputSupplier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

public class TestCaseSupplier implements InputSupplier<BufferedReader>
{
  private final String s;
  public TestCaseSupplier(String s)
  {
    this.s=s;
  }

  @Override
  public BufferedReader getInput() throws IOException
  {
    StringBuilder buffer = new StringBuilder();
    buffer.append(s);
    BufferedReader br = new BufferedReader(new StringReader(buffer.toString()));
    return br;
  }
}
