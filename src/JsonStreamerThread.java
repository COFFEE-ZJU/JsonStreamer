import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

import operators.Operator;

import parse.APIParser;
import parse.QueryParser;
import scheduler.Scheduler;
import utils.UserInfo;
import IO.JStreamOutput;
import IO.SocketStreamOutput;
import constants.Constants.ConnectionCmds;
import constants.JsonStreamerException;


public class JsonStreamerThread implements Runnable{
	private enum State{USER, PW, VERIFIED, IN_QUERY};
	private Socket socket;
	private int threadNo;
	private List<Operator> allOperators;
	public JsonStreamerThread(Socket s, int no){
		socket = s;
		threadNo = no;
		allOperators = new LinkedList<Operator>();
	}
	
	private void registerQueryFromSocket(String query, OutputStream outputStream) throws JsonStreamerException, IOException{
		JStreamOutput output = new SocketStreamOutput(outputStream);
		String api = QueryParser.parseToAPI(query);
		List<Operator> opList = new APIParser().parse(api, output);
		
		outputStream.write("Query Registered!\r\n".getBytes());
		Scheduler.getInstance().addOperators(opList);
		allOperators.addAll(opList);
		
	}
	
	@Override
	public void run() {
		try {
			System.out.println("query arrived, thread "+threadNo+" started");
			InputStream in = socket.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			OutputStream out = socket.getOutputStream();
//			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
			State state = State.USER;
			String userName = null, password;
			StringBuffer queryBuffer = null;
			boolean inProcess = true;
			
			while(inProcess){
				line = br.readLine();
				if(line == null){
					inProcess = false;
					out.write("Connection failed!\r\n".getBytes());
					break;
				}
				else if(line.equals("")) continue;
				switch (state) {
				case USER:
					if(!line.startsWith(ConnectionCmds.USER_PREFIX)){
						inProcess = false;
						out.write("Expecting User Name\r\n".getBytes());
					}
					else{
						userName = line.substring(ConnectionCmds.USER_PREFIX.length());
						state = State.PW;
					}
					break;
					
				case PW:
					if(!line.startsWith(ConnectionCmds.PW_PREFIX)){
						inProcess = false;
						out.write("Expecting Password\r\n".getBytes());
					}
					else{
						password = line.substring(ConnectionCmds.PW_PREFIX.length());
						if(UserInfo.getInstance().verify(userName, password)){
							out.write("Verification Succeeded\r\n".getBytes());
							state = State.VERIFIED;
						}
						else{
							inProcess = false;
							out.write("Verification Failed!\r\n".getBytes());
						}
					}
					break;
					
				case VERIFIED:
					if(line.startsWith(ConnectionCmds.QUERY_START)){
						queryBuffer = new StringBuffer();
						state = State.IN_QUERY;
					}
					else if(line.startsWith(ConnectionCmds.QUIT)){
						out.write("Bye!\r\n".getBytes());
						inProcess = false;
					}
					else{
						inProcess = false;
						System.err.println(line);
						out.write("Wrong Command\r\n".getBytes());
					}
					break;
					
				case IN_QUERY:
					if(line.startsWith(ConnectionCmds.QUERY_PREFIX)){
						queryBuffer.append(line.substring(ConnectionCmds.QUERY_PREFIX.length()));
						queryBuffer.append("\r\n");
					}
					else if(line.startsWith(ConnectionCmds.QUERY_END)){
						String query = queryBuffer.toString();
						try{
							registerQueryFromSocket(query, out);
						} catch (JsonStreamerException e){
							inProcess = false;
							e.printStackTrace();
							out.write((e.getClass().toString()+": "+e.getMessage()+"\r\n").getBytes());
						}
						state = State.VERIFIED;
					}
					else{
						inProcess = false;
						System.err.println(line);
						out.write("Wrong Command\r\n".getBytes());
					}
					break;
					
				}
			}
			
			if(! allOperators.isEmpty()){
				if(! Scheduler.getInstance().removeOperators(allOperators))
					throw new RuntimeException("something wrong!!!");
			}
			out.flush();
	        out.close();
	        in.close();
	        socket.close();
	        
	        System.out.println("query excuted, thread "+threadNo+" ended");
//	        synchronized ((Object)threadNum) {
//				threadNum --;
//			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
