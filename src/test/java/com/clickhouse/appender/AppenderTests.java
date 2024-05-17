package com.clickhouse.appender;

import com.clickhouse.appender.manager.LogBufferManager;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.DefaultLogEventFactory;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class AppenderTests {

  @InjectMocks ClickHouseAppender clickHouseAppender;
  @Mock Marker marker;
  @Mock LogBufferManager logBufferManager;

  @Test
  public void appenderCallsInsertMethod() {
    DefaultLogEventFactory factory = new DefaultLogEventFactory();

    LogEvent logEvent =
        factory.createEvent(
            "TestLogger",
            marker,
            "TestClass",
            Level.INFO,
            new SimpleMessage("TEST INFORMATION #1"),
            null,
            null);

    clickHouseAppender.append(logEvent);

    String serializedEvent = (String) clickHouseAppender.getLayout().toSerializable(logEvent);

    if (serializedEvent.endsWith(System.lineSeparator())) {
      serializedEvent =
          serializedEvent.substring(0, serializedEvent.length() - System.lineSeparator().length());
    }

    verify(logBufferManager).insertLogMsg(logEvent.getTimeMillis(), serializedEvent);
  }
}
