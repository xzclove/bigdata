package com.ibeifeng.zookeeper.test.configuration;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ConfigurationService {
	
	private static final int SESSION_TIMEOUT = 5000;
	private volatile String configurationInfo = new String("initial conf");
	
	public String getConfigurationInfo() {
		return configurationInfo;
	}

	private CountDownLatch connectSignal = new CountDownLatch(1);
	
	public ConfigurationService(){
		try {
			ZooKeeper zk = new ZooKeeper("",SESSION_TIMEOUT,new Watcher(){

				@Override
				public void process(WatchedEvent event) {
					if(event.getState() == KeeperState.SyncConnected){
						connectSignal.countDown();
					}
				}
			});
			connectSignal.await();
			String confNode = "/test_configuration";
			Stat stat = zk.exists(confNode, true);
			
			if(stat == null){
				zk.create(confNode, configurationInfo.getBytes(), 
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			
			watchZNode( zk,confNode);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}

	private void watchZNode(final ZooKeeper zk,final String znode) {
		try {
			byte[] confData = zk.getData(znode, new Watcher(){
				@Override
				public void process(WatchedEvent event) {
					if(event.getType() == EventType.NodeDataChanged){
						watchZNode(zk,znode);
					}
				}
			}, null);
			String conf = new String(confData);
			this.configurationInfo = conf;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
