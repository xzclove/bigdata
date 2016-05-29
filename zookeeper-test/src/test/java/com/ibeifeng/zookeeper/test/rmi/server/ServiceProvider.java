package com.ibeifeng.zookeeper.test.rmi.server;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.ibeifeng.zookeeper.test.rmi.Constants;
/**
 * 服务注册类
 * @author hadoop
 *
 */
public class ServiceProvider {
	
	private CountDownLatch connectSignal = new CountDownLatch(1);
	
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
			throw new RuntimeException("连接zookeeper服务失败" + e.getMessage());
		}
	}
	/**
	 * 发布服务并且向zookeeper注册
	 * @param remote
	 * @param host
	 * @param port
	 */
	public void publish(Remote remote,String host,int port){
		String uri = String.format("rmi://%s:%d/%s", host,port,remote.getClass().getName());
		try {
			LocateRegistry.createRegistry(port);
			Naming.bind(uri, remote);
			// 绑定成功后，在zookeeper上创建znode
			ZooKeeper zk = this.connectServer();
			
			Stat stat = zk.exists(Constants.ZK_REGISTRY_PATH, false);
			if(stat == null){
				zk.create(Constants.ZK_REGISTRY_PATH, null, 
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			
			zk.create(Constants.ZK_PROVIDER_PATH, uri.getBytes(), 
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (Exception e) {
			try {
				Naming.unbind(uri);
			} catch (RemoteException e1) {
				e1.printStackTrace();
			} catch (MalformedURLException e1) {
				e1.printStackTrace();
			} catch (NotBoundException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
			System.err.println("注册服务失败" + e.getMessage());
			//throw new RuntimeException("注册服务失败" + e.getMessage());
		}
	}

}
