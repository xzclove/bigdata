package com.ibeifeng.zookeeper.test.rmi.server;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import com.ibeifeng.zookeeper.test.rmi.OrderService;

public class OrderServiceImpl extends  UnicastRemoteObject implements OrderService {
	
	private static final long serialVersionUID = 2705281256105914627L;

	protected OrderServiceImpl() throws RemoteException {
		
	}

	@Override
	public String pay(String orderInfo)  throws RemoteException{
		System.out.println("订单信息：" + orderInfo);
		return "订单支付成功";
	}

}
