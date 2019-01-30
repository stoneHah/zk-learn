package com.stone.learn.zklearn.custome;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * ${DESCRIPTION}
 *
 * @author qun.zheng
 * @create 2019/1/30
 **/
@RunWith(SpringRunner.class)
@SpringBootTest
public class ZookeeperConstructTest {

    private static final String hosts = "192.168.10.210:2181,192.168.10.210:2182,192.168.10.210:2183";

    @Test
    public void testConstruct() throws IOException {
        CountDownLatch single = new CountDownLatch(1);

        ZooKeeper zk = new ZooKeeper(hosts, 5000, new CustomerWatcher(single));
        System.out.println(zk.getState());
        try {
            single.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Zookeeper session established");
    }

    @Test
    public void testConstructWithSID_PWD() throws Exception {
        CountDownLatch single = new CountDownLatch(1);

        ZooKeeper zk = new ZooKeeper(hosts, 5000, new CustomerWatcher(single));
        single.await();

        long sessionId = zk.getSessionId();
        byte[] sessionPasswd = zk.getSessionPasswd();

        zk = new ZooKeeper(hosts, 5000, new CustomerWatcher(single), 1l, "test".getBytes());
        zk = new ZooKeeper(hosts, 5000, new CustomerWatcher(single), sessionId, sessionPasswd);

        TimeUnit.SECONDS.sleep(10);

    }

    private class  CustomerWatcher implements Watcher{
        CountDownLatch single;
        public CustomerWatcher( CountDownLatch single) {
            this.single = single;
        }

        @Override
        public void process(WatchedEvent event) {
            System.out.println("receive watch event:" + event);
            if (event.getState() == KeeperState.SyncConnected) {
                single.countDown();
            }
        }
    }



}
