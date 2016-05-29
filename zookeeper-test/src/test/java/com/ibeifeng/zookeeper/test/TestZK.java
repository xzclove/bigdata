package com.ibeifeng.zookeeper.test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestZK {
	
	private static final String HOST = "hadoop01-senior.ibeifeng.com:2181";
	private static final int SESSION_TIME = 5000;
	
	private CountDownLatch connectSignal = new CountDownLatch(1);
	
	private ZooKeeper zk;
	
	@Before
	public void setup(){
		try {
			zk = new ZooKeeper(HOST,SESSION_TIME,new Watcher(){

				@Override
				public void process(WatchedEvent event) {
					if(event.getState() == KeeperState.SyncConnected){
						connectSignal.countDown();
					}
				}
			});
			connectSignal.await();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void getChildZnodes(){
		try {
			List<String> childZnodes = zk.getChildren("/", true);
			for(String znode : childZnodes){
				System.out.println(znode);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testCreateEphemeralZNode(){
		try {
			String znodeName = 
					zk.create("/rmi/lock", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			System.out.println(znodeName);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testCreateEphemeralZNode2(){
		try {
			zk.getChildren("/rmi", new Watcher(){

				@Override
				public void process(WatchedEvent event) {
					if(event.getType() == EventType.NodeChildrenChanged){
						System.out.println("该节点有子znode变动");
					}
				}
				
			});
			zk.create("/rmi/lock", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	@After
	public void after(){
		if(zk != null){
			try {
				zk.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
