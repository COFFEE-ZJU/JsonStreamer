package utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

import json.JsonSchema;

import com.google.gson.Gson;

import constants.Constants;
import constants.SystemErrorException;

public class SchemaServer extends StoppableThread{
	private static SchemaServer schemaServer = null;
	private ServerSocket ss;
	private Socket s;
	
	private SchemaServer(){}
	
	public static SchemaServer getInstance(){
		if(schemaServer == null) schemaServer = new SchemaServer();
		
		return schemaServer;
	}
	
	class JsonSchemaQuery{
		String type;
		String wrapper_name;
	}
	
	@Override
	protected void initialize() throws Exception {
		System.out.println("SchemaServer starts serving");
		ss = new ServerSocket(Constants.SCHEMA_SERVER_PORT);
		ss.setSoTimeout(Constants.SERVER_ACCEPT_TIMEOUT);
	}

	@Override
	protected void inLoop() throws Exception {
		try{
			s = ss.accept();
		} catch (SocketTimeoutException e) {
			return;
		}
		new ExecThread(s).start();
	}

	@Override
	protected void finish() throws Exception {
		System.out.println("SchemaServer stops serving");
	}
	
	@Override
	protected void inException(Exception e) {
		System.err.println("failed to start SchemaServer");
		e.printStackTrace();
		System.exit(1);
	}
	
//	@Override
//	public void run(){
//		
//		try {
//			ss = new ServerSocket(Constants.SCHEMA_SERVER_PORT);
//			ss.setSoTimeout(Constants.SERVER_ACCEPT_TIMEOUT);
//			Socket s;
//			while(inExecution){
//				try{
//	    			s = ss.accept();
//	    		} catch (SocketTimeoutException e) {
//					continue;
//				}
//				new ExecThread(s).start();
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//			System.err.println("failed to start SchemaServer");
//			System.exit(1);
//		}
//	}
	class ExecThread extends Thread{
		Socket socket;
		public ExecThread(Socket s){
			socket = s;
		}
		
		@Override
		public void run(){
			try {
				InputStream in = socket.getInputStream();
				OutputStream out = socket.getOutputStream();
				byte[] b = new byte[1024];
				int len = in.read(b);
				//in.close();
				String jsonString = new String(b,0,len);
				System.out.println(jsonString);
				JsonSchemaQuery query = new Gson().fromJson(jsonString, JsonSchemaQuery.class);
				JsonSchema schema = JsonSchemaUtils.getSchemaByWrapperName(query.wrapper_name);
				
				//System.out.println(outString);
				out.write(Constants.gson.toJson(schema).getBytes());
				out.flush();
				//out.close();
				
				socket.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (SystemErrorException e) {
				e.printStackTrace();
			}
		}
	}

}
