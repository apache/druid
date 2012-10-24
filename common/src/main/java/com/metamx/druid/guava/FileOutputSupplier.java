package com.metamx.druid.guava;

import com.google.common.io.OutputSupplier;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
*/
public class FileOutputSupplier implements OutputSupplier<OutputStream>
{
  private final File file;
  private final boolean append;

  public FileOutputSupplier(File file, boolean append)
  {
    this.file = file;
    this.append = append;
  }

  @Override
  public OutputStream getOutput() throws IOException
  {
    return new FileOutputStream(file, append);
  }

  public File getFile()
  {
    return file;
  }
}
