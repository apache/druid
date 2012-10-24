package com.metamx.druid.kv;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 */
public interface IOPeon
{
  public OutputStream makeOutputStream(String filename) throws IOException;
  public InputStream makeInputStream(String filename) throws IOException;
  public void cleanup() throws IOException;
}
