package com.metamx.druid.kv;

import com.google.common.collect.Maps;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
*/
public class TmpFileIOPeon implements IOPeon
{
  Map<String, File> createdFiles = Maps.newLinkedHashMap();

  @Override
  public OutputStream makeOutputStream(String filename) throws IOException
  {
    File retFile = createdFiles.get(filename);
    if (retFile == null) {
      retFile = File.createTempFile("filePeon", filename);
      createdFiles.put(filename, retFile);
    }
    return new BufferedOutputStream(new FileOutputStream(retFile));
  }

  @Override
  public InputStream makeInputStream(String filename) throws IOException
  {
    final File retFile = createdFiles.get(filename);

    return retFile == null ? null : new FileInputStream(retFile);
  }

  @Override
  public void cleanup() throws IOException
  {
    for (File file : createdFiles.values()) {
      file.delete();
    }
    createdFiles.clear();
  }
}
