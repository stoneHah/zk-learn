package com.stone.learn.zklearn.custome;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.List;
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
public class ZookeeperAPITest {

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

    /**
     * 同步创建znode
     * @throws Exception
     */
    @Test
    public void testCreatePathSync() throws Exception {
        CountDownLatch single = new CountDownLatch(1);

        ZooKeeper zk = new ZooKeeper(hosts, 5000, new CustomerWatcher(single));
        single.await();

        String path1 = zk.create("/zk-test-ephemeral-", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Success create znode:" + path1);

        String path2 = zk.create("/zk-test-ephemeral-", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Success create znode:" + path2);
    }

    /**
     * 异步方式创建znode
     * @throws Exception
     */
    @Test
    public void testCreatePathAsync() throws Exception {
        CountDownLatch single = new CountDownLatch(1);

        ZooKeeper zk = new ZooKeeper(hosts, 5000, new CustomerWatcher(single));
        single.await();

        zk.create("/zk-test-ephemeral-", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,new PrintStringCallback(),"i am a context");
        zk.create("/zk-test-ephemeral-", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,new PrintStringCallback(),"i am a context");
        zk.create("/zk-test-ephemeral-", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,new PrintStringCallback(),"i am a context");

        TimeUnit.SECONDS.sleep(5);
    }

    class PrintStringCallback implements StringCallback{

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            System.out.println(String.format("create path result:[%d,%s,%s,real path name: %s]",rc,path,ctx.toString(),name));
        }
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


    private static ZooKeeper zk = null;
    @Test
    public void testGetChildren() throws Exception {
        CountDownLatch single = new CountDownLatch(1);

        String path = "/zk-book";

        zk = new ZooKeeper(hosts, 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if(KeeperState.SyncConnected == event.getState()){
                    if (event.getType() == EventType.None && event.getPath() == null) {
                        single.countDown();
                    }else if (event.getType() == EventType.NodeChildrenChanged) {
                        try {
                            System.out.println("reget child:" + zk.getChildren(event.getPath(),true));
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        single.await();

        zk.delete(path,-1);

        zk.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(path + "/c1", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        List<String> children = zk.getChildren(path, true);
        System.out.println(children);

        zk.create(path + "/c2", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        TimeUnit.SECONDS.sleep(5);
    }


}
