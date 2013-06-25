package druid.examples.webStream;

import com.google.common.io.InputSupplier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

public class TestCaseSupplier implements InputSupplier<BufferedReader>
{
  private final ArrayList<String> inputList = new ArrayList<String>();
  public TestCaseSupplier(String s){
    inputList.add(s);
  }
  @Override
  public BufferedReader getInput() throws IOException
  {
    StringBuilder buffer = new StringBuilder();
    for (String current : inputList) {
      buffer.append(current).append('\n');
    }

    BufferedReader br = new BufferedReader(new StringReader(buffer.toString()));
    return br;
  }
}
