package com.ibeifeng.zookeeper.test.rmi.server;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

public class RMIServer {
	
	public static void main(String[] args) throws RemoteException, MalformedURLException, AlreadyBoundException{
		LocateRegistry.createRegistry(9999);
		//String uri = "rmi://hadoop01-senior.ibeifeng.com:9999/com.ibeifeng.zookeeper.test.rmi.server.OrderServiceImpl";
		String uri = "rmi://hadoop01-senior.ibeifeng.com:9999/OrderServiceImpl";
		
		Naming.bind(uri, new OrderServiceImpl());
	}

}
