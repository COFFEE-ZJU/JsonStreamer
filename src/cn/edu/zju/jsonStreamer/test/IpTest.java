package cn.edu.zju.jsonStreamer.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class IpTest {

	/**
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException {
		InetAddress addr = InetAddress.getLocalHost();
		String ip=addr.getHostAddress().toString();
		System.out.println(ip);
	}

}
