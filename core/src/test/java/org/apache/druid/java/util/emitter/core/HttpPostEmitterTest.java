package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test {@link HttpPostEmitter} class.
 */
public class HttpPostEmitterTest
{

  private static final ObjectMapper objectMapper = new ObjectMapper()
  {
    @Override
    public byte[] writeValueAsBytes(Object value)
    {
      return Ints.toByteArray(((IntEvent) value).index);
    }
  };

  private final MockHttpClient httpClient = new MockHttpClient();

  @Before
  public void setup()
  {
    httpClient.setGoHandler(new GoHandler()
    {
      @Override
      protected ListenableFuture<Response> go(Request request)
      {
        return GoHandlers.immediateFuture(EmitterTest.okResponse());
      }
    });
  }


  @Test(expected = ClassCastException.class)
  @SuppressWarnings("unchecked")
  public void testRecoveryEmitAndReturnBatch()
      throws InterruptedException, IOException, NoSuchFieldException, IllegalAccessException
  {
    HttpEmitterConfig config = new HttpEmitterConfig.Builder("http://foo.bar")
        .setFlushMillis(100)
        .setFlushCount(4)
        .setBatchingStrategy(BatchingStrategy.ONLY_EVENTS)
        .setMaxBatchSize(1024 * 1024)
        .setBatchQueueSizeLimit(1000)
        .build();
    final HttpPostEmitter emitter = new HttpPostEmitter(config, httpClient, objectMapper);
    emitter.start();

    // emit first event
    emitter.emitAndReturnBatch(new IntEvent());
    Thread.sleep(1000L);

    // get concurrentBatch reference and set value to lon as if it would fail while
    // HttpPostEmitter#onSealExclusive method invocation.
    Field concurrentBatch = emitter.getClass().getDeclaredField("concurrentBatch");
    concurrentBatch.setAccessible(true);
    ((AtomicReference<Object>) concurrentBatch.get(emitter)).getAndSet(1L);
    // something terrible happened previously so that batch has to recover

    // emit second event
    emitter.emitAndReturnBatch(new IntEvent());

    emitter.flush();
    emitter.close();
  }

}
