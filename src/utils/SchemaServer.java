package utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import json.JsonSchema;

import com.google.gson.Gson;

import constants.Constants;
import constants.SystemErrorException;

public class SchemaServer extends Thread{
	
	class JsonSchemaQuery{
		String type;
		String wrapper_name;
	}
	
	@Override
	public void run(){
		ServerSocket ss;
		try {
			ss = new ServerSocket(Constants.SCHEMA_SERVER_PORT);
			Socket s;
			while(true){
				s = ss.accept();
				new ExecThread(s).start();
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
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
