package com.ibeifeng.zookeeper.test.rmi.client;

import java.rmi.Naming;
import java.rmi.Remote;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import com.ibeifeng.zookeeper.test.rmi.Constants;

public class ServiceConsumer {
	
	private CountDownLatch connectSignal = new CountDownLatch(1);
	
	private volatile List<String> urls = new ArrayList<String>();
	
	public ServiceConsumer(){
		// 获取zookeeper连接
		ZooKeeper zk = this.connectServer();
		if(zk != null){
			// 监听临时节点
			this.watchNode(zk);
		}
	}
	
	public <T extends Remote> T lookup(){
		T service  = null;
		int size = urls.size();
		if(size > 0){
			String url = urls.get(ThreadLocalRandom.current().nextInt(size));
			System.out.println("连接到------>" + url);
			service  = lookupService(url);
		}
		return service;
	}
	/**
	 * 查找服务
	 * @param url
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private <T> T lookupService(String url) {
		T service = null;
		try {
			service = (T) Naming.lookup(url);
		} catch (Exception e) {
			e.printStackTrace();
			if(urls.size() != 0){
				// 如果出现连接异常，则直接连接第一个
				service = this.lookupService(urls.get(0));
			}
		} 
		return service;
	}

	/**
	 * 监听节点
	 * @param zk
	 */
	private void watchNode(final ZooKeeper zk){
		try {
			List<String> childNodes = zk.getChildren(Constants.ZK_REGISTRY_PATH, new Watcher(){
				@Override
				public void process(WatchedEvent event) {
					if(event.getType() == EventType.NodeChildrenChanged){
						watchNode(zk);
					}
				}
			});
			List<String> urls = new ArrayList<String>();
			for(String childNode : childNodes){
				byte[] dataBytes = 
						zk.getData(Constants.ZK_REGISTRY_PATH + "/" + childNode, false, null);
				urls.add(new String(dataBytes));
			}
			this.urls = urls;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private ZooKeeper connectServer(){
		try {
			ZooKeeper zk = new ZooKeeper(Constants.ZK_HOSTS,Constants.SESSION_TIME,new Watcher(){
				@Override
				public void process(WatchedEvent event) {
					if(event.getState() == KeeperState.SyncConnected){
						connectSignal.countDown();
					}
				}
			});
			connectSignal.await();
			return zk;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("连接服务器失败");
		}
	}

}
