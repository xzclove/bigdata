package com.ibeifeng.zookeeper.test.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * 订单服务
 * @author hadoop
 *
 */
public interface OrderService extends Remote{
	/**
	 * 支付
	 * @param orderInfo
	 * @return
	 */
	String pay(String orderInfo ) throws RemoteException;

}
