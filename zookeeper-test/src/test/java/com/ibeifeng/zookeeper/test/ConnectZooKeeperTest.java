package com.ibeifeng.zookeeper.test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
/**
 * 
 * @author hadoop
 *
 */
public class ConnectZooKeeperTest implements Watcher{
	
	private static final String URI = "hadoop01-senior.ibeifeng.com:2181";
	private static final int SESSION_TIME = 5000;
	
	private CountDownLatch connectSignal = new CountDownLatch(1);
	
	private ZooKeeper zookeeper;
	
	public void createGroup(String groupName){
		try {
			String createdPath = 
					zookeeper.create("/" + groupName, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println(createdPath);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	public void connect(){
		try {
			zookeeper = new ZooKeeper(URI,SESSION_TIME,this);
			connectSignal.await();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if(event.getState() == KeeperState.SyncConnected){
			connectSignal.countDown();
		}
	}
	
	public void close(){
		try {
			if(zookeeper != null)
				zookeeper.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void  main(String[] args){
		ConnectZooKeeperTest test = new ConnectZooKeeperTest();
		test.connect();
		test.createGroup("rmi2");
		test.close();
	}
	
}
