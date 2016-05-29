package com.ibeifeng.zookeeper.test.rmi.server;

import java.rmi.RemoteException;

public class Server {

	public static void main(String[] args) throws RemoteException{
		
		ServiceProvider serviceProvider = new ServiceProvider();
		serviceProvider.publish(new OrderServiceImpl(), "localhost", 22235);
		
	}
}
