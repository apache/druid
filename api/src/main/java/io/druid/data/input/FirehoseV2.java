/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.data.input;

import java.io.Closeable;
/**
 * This is an interface that holds onto the stream of incoming data.  Realtime data ingestion is built around this
 * abstraction.  In order to add a new type of source for realtime data ingestion, all you need to do is implement
 * one of these and register it with the Main.
 *
 * In contrast to Firehose v1 version, FirehoseV2 will always operate in a "peek, then advance" manner.
 * And the intended usage patttern is
 * 1. Call start()
 * 2. Read currRow()
 * 3. Call advance()
 * 4. If index should be committed: commit()
 * 5. GOTO 2
 *
 * Note that commit() is being called *after* advance.
 * 
 * This object acts a lot like an Iterator, but it doesn't extend the Iterator interface because it extends
 * Closeable and it is very important that the close() method doesn't get forgotten, which is easy to do if this
 * gets passed around as an Iterator.
 * <p>
 * The implementation of this interface only needs to be minimally thread-safe. The methods ##start(), ##advance(),
 * ##currRow() and ##makeCommitter() are all called from the same thread.  ##makeCommitter(), however, returns a callback
 * which will be called on another thread, so the operations inside of that callback must be thread-safe.
 * </p>
 */
public interface FirehoseV2 extends Closeable
{
    /**
     * For initial start
     * */
    void start() throws Exception;

    /**
     * Advance the firehose to the next offset.  Implementations of this interface should make sure that
     * if advance() is called and throws out an exception, the next call to currRow() should return a 
     * null value.
     * 
     * @return true if and when there is another row available, false if the stream has dried up
     */
    public boolean advance();

    /**
     * @return The current row
     */
    public InputRow currRow() ;

    /**
     * Returns a Committer that will "commit" everything read up to the point at which makeCommitter() is called.
     *
     * This method is called when the main processing loop starts to persist its current batch of things to process.
     * The returned committer will be run when the current batch has been successfully persisted
     * and the metadata the committer carries can also be persisted along with segment data. There is usually
     * some time lag between when this method is called and when the runnable is run.  The Runnable is also run on
     * a separate thread so its operation should be thread-safe.
     * 
     * Note that "correct" usage of this interface will always call advance() before commit() if the current row
     * is considered in the commit.
     *
     * The Runnable is essentially just a lambda/closure that is run() after data supplied by this instance has
     * been committed on the writer side of this interface protocol.
     * <p>
     * A simple implementation of this interface might do nothing when run() is called,
     * and save proper commit information in metadata
     * </p>
     */
    public Committer makeCommitter();
}
