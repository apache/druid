package io.druid.segment.smooth;

import com.metamx.common.io.smoosh.SmooshedWriter;
import io.druid.segment.store.Directory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by twj on 2016/11/28.
 */
public class DirectorySmoothWriter implements SmooshedWriter
{
  private Directory directory;

  public DirectorySmoothWriter(Directory directory){

  }

  @Override
  public int write(InputStream inputStream) throws IOException
  {
    return 0;
  }

  @Override
  public int write(ByteBuffer src) throws IOException
  {
    return 0;
  }

  @Override
  public boolean isOpen()
  {
    return false;
  }

  @Override
  public void close() throws IOException
  {

  }
}
