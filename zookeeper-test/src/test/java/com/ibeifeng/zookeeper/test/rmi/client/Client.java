package com.ibeifeng.zookeeper.test.rmi.client;

import java.rmi.RemoteException;

import com.ibeifeng.zookeeper.test.rmi.OrderService;

public class Client {
	
	public static void main(String[] args) throws RemoteException{
		
		ServiceConsumer consumer = new ServiceConsumer();
		OrderService orderService = consumer.lookup();
		String response  = orderService.pay("amt:334.55,name:zhangsan");
		System.out.println(response);
		
	}

}
