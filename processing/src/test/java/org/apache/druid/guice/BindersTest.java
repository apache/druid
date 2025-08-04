package org.apache.druid.guice;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

class BindersTest
{

  @Test
  public void test_bindTaskLogs()

  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.logs.type", "noop");
    
    Injector injector = Guice.createInjector(
        binder -> {
          binder.bind(Properties.class).toInstance(props);
          PolyBind.createChoice(binder, "druid.indexer.logs.type", Key.get(TaskLogs.class), null);
          PolyBind.createChoiceWithDefault(binder, "druid.indexer.logs.defaultType", Key.get(TaskLogs.class, Names.named("defaultType")), "noop");
          Binders.bindTaskLogs(binder, "noop", NoopTaskLogs.class);
        }
    );

    TaskLogs taskLogs = injector.getInstance(TaskLogs.class);
    Assertions.assertInstanceOf(NoopTaskLogs.class, taskLogs);
    
    TaskLogs namedTaskLogs = injector.getInstance(Key.get(TaskLogs.class, Names.named("noop")));
    Assertions.assertInstanceOf(NoopTaskLogs.class, namedTaskLogs);
    
    TaskLogs defaultTypeTaskLogs = injector.getInstance(Key.get(TaskLogs.class, Names.named("defaultType")));
    Assertions.assertInstanceOf(NoopTaskLogs.class, defaultTypeTaskLogs);
  }
}
