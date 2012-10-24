package com.metamx.druid.realtime;

import com.metamx.druid.input.InputRow;

import java.io.Closeable;

/**
 * This is an interface that holds onto the stream of incoming data.  Realtime data ingestion is built around this
 * abstraction.  In order to add a new type of source for realtime data ingestion, all you need to do is implement
 * one of these and register it with the RealtimeMain.
 *
 * This object acts a lot like an Iterator, but it doesn't not extend the Iterator interface because it extends
 * Closeable and it is very important that the close() method doesn't get forgotten, which is easy to do if this
 * gets passed around as an Iterator.
 * <p>
 * The implementation of this interface only needs to be minimally thread-safe. There is one callback provided
 * by ##commit() which will be called on another thread.  But ##hasMore() and ##nextRow() will always only be called
 * by a single thread.
 * </p>
 */
public interface Firehose extends Closeable
{
  /**
   * Returns whether there are more rows to process.  This is used to indicate that another item is immediately
   * available via ##nextRow().  Thus, if the stream is still available but there are no new messages on it, this call
   * should block until a new message is available.
   *
   * If something happens such that the stream is no longer available, this should return false.
   *
   * @return true if and when there is another row available, false if the stream has dried up
   */
  public boolean hasMore();

  /**
   * The next row available.  Should only be called if hasMore returns true.
   *
   * @return The next row
   */
  public InputRow nextRow();

  /**
   * Returns a runnable that will "commit" everything read up to the point at which commit() is called.  This is
   * often equivalent to everything that has been read since the last commit() call (or instantiation of the object),
   * but doesn't necessarily have to be.
   *
   * This method is called when the main processing loop starts to persist its current batch of things to process.
   * The returned runnable will be run when the current batch has been successfully persisted, there is usually
   * some time lag between when this method is called and when the runnable is run.  The Runnable is also run on
   * a separate thread so its operation should be thread-safe.
   *
   * The Runnable is essentially just a lambda/closure that is run() after data supplied by this instance has
   * been committed on the writer side of this interface protocol.
   * <p>
   * A simple implementation of this interface might do nothing when run() is called 
   * (in which case the same do-nothing instance can be returned every time), or 
   * a more complex implementation might clean up temporary resources that are no longer needed 
   * because of InputRows delivered by prior calls to ##nextRow().
   * </p>
   */
  public Runnable commit();
}
