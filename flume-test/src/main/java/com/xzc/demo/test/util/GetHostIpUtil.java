package com.xzc.demo.test.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * @Des 得到机器IP工具
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年5月4日 下午2:28:04
 * @Version V1.0.0
 */

public class GetHostIpUtil {
	public static boolean isWindowsOS() {
		boolean isWindowsOS = false;
		String osName = System.getProperty("os.name");
		if (osName.toLowerCase().indexOf("windows") > -1) {
			isWindowsOS = true;
		}
		return isWindowsOS;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<String> getLocalIpList() {
		List ips = new ArrayList();
		try {
			Enumeration interfaces = NetworkInterface.getNetworkInterfaces();
			while (interfaces.hasMoreElements()) {
				NetworkInterface networkInterface = (NetworkInterface) interfaces.nextElement();
				if ((!networkInterface.isLoopback()) && (!networkInterface.isVirtual()) && (networkInterface.isUp())) {
					Enumeration addresses = networkInterface.getInetAddresses();
					while (addresses.hasMoreElements()) {
						InetAddress ipv4 = (InetAddress) addresses.nextElement();
						if ((ipv4 instanceof Inet4Address))
							ips.add(ipv4.toString().replace("/", ""));
					}
				}
			}
		} catch (Exception e) {
		}
		return ips;
	}
}