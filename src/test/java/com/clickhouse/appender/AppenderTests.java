package com.clickhouse.appender;

import com.clickhouse.appender.manager.LogBufferManager;
import com.clickhouse.appender.util.ConnectionSettings;
import com.clickhouse.client.ClickHouseNode;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.DefaultLogEventFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class AppenderTests {

  ClickHouseAppender clickHouseAppender;

  @Mock ConnectionSettings connectionSettings;
  @Mock ClickHouseNode node;

  @Mock Filter filter;

  Layout<String> layout;

  @Mock Marker marker;

  @Mock LogBufferManager logBufferManager;

  @BeforeEach
  void initClickHouseAppender() throws IOException {

    layout = PatternLayout.createDefaultLayout();

    when(connectionSettings.initClickHouseConnection()).thenReturn(node);

    connectionSettings.initClickHouseConnection(
        anyString(),
        anyInt(),
        anyString(),
        anyString(),
        anyString(),
        anyString(),
        anyString(),
        anyString());

    clickHouseAppender =
        ClickHouseAppender.createAppender(
            "ClickHouseAppender",
            filter,
            layout,
            false,
            8192,
            5,
            "test_table",
            3,
            5,
            connectionSettings);

    clickHouseAppender.setLogBufferManager(logBufferManager);
  }

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
