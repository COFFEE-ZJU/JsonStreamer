package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class SocketTest extends Thread{
	
	@Override
	public void run(){
		try {
			ServerSocket ss = new ServerSocket(1003);
			Socket s = ss.accept();
			InputStream is = s.getInputStream();
			OutputStream os = s.getOutputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			byte[] b = new byte[1024];
			for(int i=0;i<10;i++){
				String line = br.readLine();
				System.out.println("incoming: "+line);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
//		new SocketTest().start();
//		new Client();
		String ss = "test:";
		System.out.println((ss+"any").substring(ss.length()));
	}
}

class Client {
	public Client(){
		try {
			Socket s = new Socket("localhost", 1003);
			OutputStream os = s.getOutputStream();
			os.write("send1".getBytes());
			os.write("send2\n".getBytes());
			os.flush();
			os.write("send3\n".getBytes());
			os.write("send4".getBytes());
			
			os.close();
			s.close();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
