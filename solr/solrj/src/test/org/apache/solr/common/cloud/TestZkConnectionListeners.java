/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.common.cloud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestZkConnectionListeners {

  class MockConnectionStrategy extends ZkConnectionFactory {

    List<Watcher> watchers = new ArrayList<>();
    List<SolrZooKeeper> keepers = new ArrayList<>();

    public MockConnectionStrategy() {
      super("", -1);
    }

    @Override
    protected SolrZooKeeper createSolrZooKeeper(Watcher watcher) throws IOException {
      SolrZooKeeper newKeeper = mock(SolrZooKeeper.class);
      when(newKeeper.getSessionId()).thenReturn((long)keepers.size());
      keepers.add(newKeeper);
      watchers.add(watcher);
      connect();
      return newKeeper;
    }

    void connect() {
      watchers.get(watchers.size() - 1)
          .process(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.SyncConnected, ""));
    }

    void hiccup() {
      watchers.get(watchers.size() - 1)
          .process(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected, ""));
      watchers.get(watchers.size() - 1)
          .process(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.SyncConnected, ""));
    }

    void expire() {
      watchers.get(watchers.size() - 1)
          .process(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, ""));
    }
  }

  class CountingConnectionListener implements ZkConnectionListener {

    int connects = 0;
    int expiries = 0;
    CountDownLatch latch;

    @Override
    public void onConnect() {
      connects++;
      if (latch != null)
        latch.countDown();
    }

    @Override
    public void onSessionExpiry() {
      expiries++;
      if (latch != null)
        latch.countDown();
    }

  }

  @Test
  public void testSessionExpiryFiresListeners() throws Exception {

    MockConnectionStrategy strategy = new MockConnectionStrategy();
    CountingConnectionListener listener = new CountingConnectionListener();

    try (SolrZkClient client = new SolrZkClient(strategy)) {
      client.registerConnectionListener(listener);
      assertEquals(1, listener.connects);
      listener.latch = new CountDownLatch(2); // calls onExpiry(), then onConnect()
      strategy.expire();

      listener.latch.await(10, TimeUnit.SECONDS);
      assertEquals(1, listener.expiries);

      // Expiry should have created a new SolrZookeeper instance
      assertEquals(2, strategy.keepers.size());
      assertEquals(2, listener.connects);
      // Old keeper should have been closed
      verify(strategy.keepers.get(0)).close();

      // Network hiccup with preserved session doesn't require a new SolrZookeeper
      // instance or call onConnect()
      strategy.hiccup();
      assertEquals(2, listener.connects);
    }

    // Last keeper should have been closed
    verify(strategy.keepers.get(1)).close();

  }

}
