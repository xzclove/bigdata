package com.ibeifeng.zookeeper.test.rmi.client;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import com.ibeifeng.zookeeper.test.rmi.OrderService;


public class RMIClient {
	
	public static void main(String[] args) throws MalformedURLException, RemoteException, NotBoundException{
		//String uri = "rmi://hadoop01-senior.ibeifeng.com:9999/com.ibeifeng.zookeeper.test.rmi.server.OrderServiceImpl";
		String uri = "rmi://hadoop01-senior.ibeifeng.com:9999/OrderServiceImpl";
		
		OrderService orderService = (OrderService) Naming.lookup(uri);
		String response = orderService.pay("id:1,amt:45.6");
		System.out.println(response);
	}

}
