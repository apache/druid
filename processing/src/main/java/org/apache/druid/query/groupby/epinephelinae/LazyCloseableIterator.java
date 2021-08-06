package org.apache.druid.query.groupby.epinephelinae;

import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import javax.inject.Provider;

/**
 * Instance of CloseableIterator that can lazily allocate its iterator based
 * on a Provider and ensure it is closed when close() is called.
 * 
 * @param <T> Type of item to return from the iterator
 */
public class LazyCloseableIterator<T> implements CloseableIterator<T>
{
  private Provider<Iterator<T>> provider;
  private Iterator<T> iterator;
  private Closeable closeable;
  private boolean closed;
  
  public LazyCloseableIterator(Provider<Iterator<T>> innerIteratorProvider) {
    provider = innerIteratorProvider;
  }

  @Override
  public boolean hasNext()
  {
    if (closed) {
      return false;
    } 
    if (iterator == null) {
      iterator = provider.get();
      if (iterator instanceof Closeable) {
        closeable = (Closeable) iterator;
      }
    }
    
    return iterator.hasNext();
  }

  @Override
  public T next()
  {
    return iterator.next();
  }

  @Override
  public void close() throws IOException
  {
    if(!closed) {
      closed = true;
      if (closeable != null) {
        closeable.close();
      }
      iterator = null;
      closeable = null;
    }
  }
}
