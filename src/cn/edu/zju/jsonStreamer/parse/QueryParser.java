package cn.edu.zju.jsonStreamer.parse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;


public class QueryParser {
	public static final int SEND_PORT = Constants.COMPILER_PORT;
	
	public static String parseToAPIByFile(String queryFileName) throws SystemErrorException{
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(Constants.QUERY_FILE_PATH + queryFileName + ".txt"));
			StringBuffer outString = new StringBuffer();
			String tmp;
			while((tmp = reader.readLine()) != null){
				outString.append(tmp+"\r\n");
			}
			
			return parseToAPI(outString.toString());
		}catch (IOException e) {
			e.printStackTrace();
			throw new SystemErrorException(e);
		}
	}
	
	public static String parseToAPI(String query) throws SystemErrorException{
		try {
			Socket socket = new Socket("localhost", SEND_PORT);
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			
			out.write(query.getBytes());
			out.flush();
			
			byte[] b = new byte[102400];
			int len = in.read(b);
			String jsonString = new String(b,0,len);
			//System.out.println(jsonString);
			
			in.close();
			out.close();
			socket.close();
			
			return jsonString;
		} catch (IOException e) {
			throw new SystemErrorException(e);
		}
	}
	
	class ExecThread extends Thread{
		String queryFileName;
		public ExecThread(String query){
			queryFileName = query;
		}
		
		@Override
		public void run(){
			try {
				Socket socket = new Socket("localhost", SEND_PORT);
				InputStream in = socket.getInputStream();
				OutputStream out = socket.getOutputStream();
				BufferedReader reader = new BufferedReader(new FileReader(Constants.QUERY_FILE_PATH + queryFileName + ".txt"));
				StringBuffer outString = new StringBuffer();
				String tmp;
				while((tmp = reader.readLine()) != null){
					outString.append(tmp+"\r\n");
				}
				out.write(outString.toString().getBytes());
				out.flush();
				
				byte[] b = new byte[10240];
				int len = in.read(b);
				String jsonString = new String(b,0,len);
				System.out.println(jsonString);
				
				in.close();
				out.close();
				socket.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public QueryParser() throws IOException{
		int i =0;
		new ExecThread("input").start();
//		while(i < 1){
//			new ExecThread("input").start();
//			i++;
//		}
	}
}
