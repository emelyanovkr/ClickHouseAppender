package com.clickhouse.appender.manager;

import com.clickhouse.appender.dao.ClickHouseLogDAO;
import com.clickhouse.appender.util.ConnectionSettings;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class LogBufferManagerTests {

  LogBufferManager spyManager;

  @Mock ConnectionSettings connectionSettings;
  @Mock ClickHouseNode node;

  @BeforeEach
  void setUp() throws IOException {
    when(connectionSettings.initClickHouseConnection()).thenReturn(node);
    when(node.getProtocol()).thenReturn(ClickHouseProtocol.HTTP);
    LogBufferManager logBufferManager =
        new LogBufferManager(8192, 1, "test", 1, 0, connectionSettings);
    spyManager = spy(logBufferManager);
  }

  @Test
  public void flushCalledWithTrueConditions() throws InterruptedException {

    doReturn(true).when(spyManager).flushRequired(anyInt(), anyLong());

    Thread bufferManagement = new Thread(() -> spyManager.bufferManagement());
    bufferManagement.start();

    Thread.sleep(500);

    verify(spyManager, timeout(5000).atLeastOnce()).flush();
  }

  @Test
  public void flushCalledTenTimesWithTrueConditions()
  {
    doReturn(true).when(spyManager).flushRequired(anyInt(), anyLong());

    Thread bufferManagement = new Thread(() -> spyManager.bufferManagement());
    bufferManagement.start();

    verify(spyManager, timeout(1000).atLeast(10)).flush();
  }
}
