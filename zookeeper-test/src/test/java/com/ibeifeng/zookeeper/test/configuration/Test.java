package com.ibeifeng.zookeeper.test.configuration;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class Test {
	
	private static final int SESSION_TIMEOUT = 5000;
	private CountDownLatch connectSignal = new CountDownLatch(1);
	
	private ZooKeeper zk = null;
	
	private void connectServer() throws IOException, InterruptedException{
		zk = new ZooKeeper("",SESSION_TIMEOUT,new Watcher(){
			@Override
			public void process(WatchedEvent event) {
				if(event.getState() == KeeperState.SyncConnected){
					connectSignal.countDown();
				}
			}
		});
		connectSignal.await();
	}
	
	private void close() throws InterruptedException{
		if(zk != null) zk.close();
	}
	
	private void changeConf(String newConf) throws KeeperException, InterruptedException{
		zk.setData("/test_configuration", newConf.getBytes(), -1);
	}
	
	public static void  main(String[] args){
		
		ConfigurationService cs = new ConfigurationService();
		System.out.println(cs.getConfigurationInfo());
		Test test = new Test();
		try {
			test.connectServer();
			test.changeConf("newconf3");
			
			System.out.println(cs.getConfigurationInfo());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}finally{
			try {
				test.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}

}
