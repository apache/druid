package org.apache.druid.guice;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.apache.druid.indexing.common.tasklogs.FileTaskLogs;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogs;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.validation.executable.ExecutableValidator;
import javax.validation.metadata.BeanDescriptor;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class IndexingServiceTaskLogsModuleTest
{

  @Test
  public void test_provideStreamer_shouldReturnFileTaskLog()
  {
    Properties props = new Properties();

    Injector injector = Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(Validator.class).toInstance(new MockValidator());
        },
        new IndexingServiceTaskLogsModule()
    );

    TaskLogs streamer = injector.getInstance(Key.get(TaskLogs.class, Names.named("streamer")));
    Assert.assertTrue(streamer instanceof FileTaskLogs);
  }

  @Test
  public void test_provideStreamer_shouldReturnNoopTaskStreamer()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.logs.type", "switching");
    props.setProperty("druid.indexer.logs.switching.defaultType", "noop");

    Injector injector = Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(Validator.class).toInstance(new MockValidator());
          Binders.bindTaskLogs(binder, "noop", NoopTaskLogs.class);
        },
        new IndexingServiceTaskLogsModule()
    );

    TaskLogs streamer = injector.getInstance(Key.get(TaskLogs.class, Names.named("streamer")));
    Assert.assertTrue(streamer instanceof NoopTaskLogs);
  }

  @Test
  public void test_provideStreamer_shouldReturnConfiguredStreamingTaskLog()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.logs.type", "switching");
    props.setProperty("druid.indexer.logs.switching.defaultType", "noop");
    props.setProperty("druid.indexer.logs.switching.streamType", "file");

    Injector injector = Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(Validator.class).toInstance(new MockValidator());
        },
        new IndexingServiceTaskLogsModule()
    );

    TaskLogs streamer = injector.getInstance(Key.get(TaskLogs.class, Names.named("streamer")));
    Assert.assertTrue(streamer instanceof FileTaskLogs);
  }

  @Test
  public void test_providePusher_shouldReturnFileTaskLog()
  {
    Properties props = new Properties();

    Injector injector = Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(Validator.class).toInstance(new MockValidator());
        },
        new IndexingServiceTaskLogsModule()
    );

    TaskLogs pusher = injector.getInstance(Key.get(TaskLogs.class, Names.named("pusher")));
    Assert.assertTrue(pusher instanceof FileTaskLogs);
  }

  @Test
  public void test_providePusher_shouldReturnNoopTaskPusher()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.logs.type", "switching");
    props.setProperty("druid.indexer.logs.switching.defaultType", "noop");

    Injector injector = Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(Validator.class).toInstance(new MockValidator());
          Binders.bindTaskLogs(binder, "noop", NoopTaskLogs.class);
        },
        new IndexingServiceTaskLogsModule()
    );

    TaskLogs pusher = injector.getInstance(Key.get(TaskLogs.class, Names.named("pusher")));
    Assert.assertTrue(pusher instanceof NoopTaskLogs);
  }

  @Test
  public void test_providePusher_shouldReturnConfiguredPushingTaskLog()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.logs.type", "switching");
    props.setProperty("druid.indexer.logs.switching.defaultType", "noop");
    props.setProperty("druid.indexer.logs.switching.pushType", "file");

    Injector injector = Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(Validator.class).toInstance(new MockValidator());
        },
        new IndexingServiceTaskLogsModule()
    );

    TaskLogs pusher = injector.getInstance(Key.get(TaskLogs.class, Names.named("pusher")));
    Assert.assertTrue(pusher instanceof FileTaskLogs);
  }

  @Test
  public void test_provideDelegate_shouldReturnFileTaskLog()
  {
    Properties props = new Properties();

    Injector injector = Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(Validator.class).toInstance(new MockValidator());
        },
        new IndexingServiceTaskLogsModule()
    );

    TaskLogs delegate = injector.getInstance(Key.get(TaskLogs.class, Names.named("reports")));
    Assert.assertTrue(delegate instanceof FileTaskLogs);
  }

  @Test
  public void test_provideDelegate_shouldReturnNoopTaskDelegate()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.logs.type", "switching");
    props.setProperty("druid.indexer.logs.switching.defaultType", "noop");

    Injector injector = Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(Validator.class).toInstance(new MockValidator());
          Binders.bindTaskLogs(binder, "noop", NoopTaskLogs.class);
        },
        new IndexingServiceTaskLogsModule()
    );

    TaskLogs delegate = injector.getInstance(Key.get(TaskLogs.class, Names.named("reports")));
    Assert.assertTrue(delegate instanceof NoopTaskLogs);
  }

  @Test
  public void test_provideDelegate_shouldReturnConfiguredReportsTaskLog()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.logs.type", "switching");
    props.setProperty("druid.indexer.logs.switching.defaultType", "noop");
    props.setProperty("druid.indexer.logs.switching.reportsType", "file");

    Injector injector = Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(Validator.class).toInstance(new MockValidator());
        },
        new IndexingServiceTaskLogsModule()
    );

    TaskLogs delegate = injector.getInstance(Key.get(TaskLogs.class, Names.named("reports")));
    Assert.assertTrue(delegate instanceof FileTaskLogs);
  }

  private static class MockValidator implements Validator
  {
    @Override
    public <T> Set<ConstraintViolation<T>> validate(T object, Class<?>... groups)
    {
      return Collections.emptySet();
    }

    @Override
    public <T> Set<ConstraintViolation<T>> validateProperty(
        T object,
        String propertyName,
        Class<?>... groups
    )
    {
      return Collections.emptySet();
    }

    @Override
    public <T> Set<ConstraintViolation<T>> validateValue(
        Class<T> beanType,
        String propertyName,
        Object value,
        Class<?>... groups
    )
    {
      return Collections.emptySet();
    }

    @Override
    public BeanDescriptor getConstraintsForClass(Class<?> clazz)
    {
      return null;
    }

    @Override
    public <T> T unwrap(Class<T> type)
    {
      return null;
    }

    @Override
    public ExecutableValidator forExecutables()
    {
      return null;
    }
  }
}
