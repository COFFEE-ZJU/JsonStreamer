import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import constants.Constants;
import operators.Operator;
import IO.FileStreamOutput;
import IO.JStreamOutput;
import IO.ScreenStreamOutput;
import IO.SocketStreamOutput;
import parse.APIParser;
import parse.QueryParser;
import scheduler.Scheduler;
import utils.SchemaServer;
import utils.UserInfo;


public class JsonStreamer {
	private int threadNum = 0;
	private enum State{USER, PW, VERIFIED, IN_QUERY};
	public JsonStreamer() throws IOException{
		ExecutorService threadPool = Executors.newFixedThreadPool(Constants.THREAD_POOL_NUM);
		ServerSocket ss = new ServerSocket(Constants.LISTEN_PORT);
    	Socket s;
    	while(true){
    		s = ss.accept();
    		threadPool.execute(new JsonStreamerThread(s,threadNum));
    		synchronized ((Object)threadNum) {
    			System.out.println("query arrived, thread "+threadNum+" started");
				threadNum ++;
			}
    	}
	}
	
	private void registerQueryFromSocket(String query, OutputStream outputStream){
		JStreamOutput output = new SocketStreamOutput(outputStream);
		String api = QueryParser.parseToAPI(query);
		Scheduler.getInstance().addOperators(new APIParser().parse(api, output));
		
	}
	
	class JsonStreamerThread implements Runnable{
    	private Socket socket;
    	private int threadNo;
    	public JsonStreamerThread(Socket s, int no){
    		socket = s;
    		threadNo = no;
    	}
    	
		@Override
		public void run() {
			try {
				InputStream in = socket.getInputStream();
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String line;
				OutputStream out = socket.getOutputStream();
				State state = State.USER;
				String userName = null, password;
				StringBuffer queryBuffer = null;
				boolean inProcess = true;
				
				while(inProcess){
					line = br.readLine();
					if(line == null){
						inProcess = false;
						out.write("Connection failed!".getBytes());
						break;
					}
					
					switch (state) {
					case USER:
						if(!line.startsWith(Constants.USER_PREFIX)){
							inProcess = false;
							out.write("Expecting User Name".getBytes());
						}
						else{
							userName = line.substring(Constants.USER_PREFIX.length());
							state = State.PW;
						}
						break;
						
					case PW:
						if(!line.startsWith(Constants.PW_PREFIX)){
							inProcess = false;
							out.write("Expecting Password".getBytes());
						}
						else{
							password = line.substring(Constants.PW_PREFIX.length());
							if(UserInfo.getInstance().verify(userName, password)){
								out.write("Verification Succeeded".getBytes());
								state = State.VERIFIED;
							}
							else{
								inProcess = false;
								out.write("Verification Failed!".getBytes());
							}
						}
						break;
						
					case VERIFIED:
						if(line.startsWith(Constants.QUERY_START)){
							queryBuffer = new StringBuffer();
							state = State.IN_QUERY;
						}
						else if(line.startsWith(Constants.QUIT)){
							
							//TODO
							out.write("Bye!".getBytes());
							inProcess = false;
						}
						else{
							inProcess = false;
							out.write("Wrong Command".getBytes());
						}
						break;
						
					case IN_QUERY:
						if(line.startsWith(Constants.QUERY_PREFIX)){
							queryBuffer.append(line.substring(Constants.QUERY_PREFIX.length()));
							queryBuffer.append("\r\n");
						}
						else if(line.startsWith(Constants.QUERY_END)){
							String query = queryBuffer.toString();
							
							//TODO
							state = State.VERIFIED;
						}
						else{
							inProcess = false;
							out.write("Wrong Command".getBytes());
						}
						break;
						
					}
				}
				
				out.flush();
		        out.close();
		        in.close();
		        socket.close();
		        
		        System.out.println("query excuted, thread "+threadNo+" ended");
		        synchronized ((Object)threadNum) {
					threadNum --;
				}
				
			} catch (Exception e) {
				// TODO: handle exception
			}
			
		}
	}
	
	public static void main(String[] args) {
		new SchemaServer().start();
		String API1 = QueryParser.parseToAPIByFile("query_1");
		String API2 = QueryParser.parseToAPIByFile("query_2");
		System.out.println(API1);
		System.out.println(API2);
		
		JStreamOutput outputStream2 = new ScreenStreamOutput();
		JStreamOutput outputStream1 = new FileStreamOutput("output_1");
		Scheduler.getInstance().addOperators(new APIParser().parse(API1, outputStream1));
		Scheduler.getInstance().addOperators(new APIParser().parse(API2, outputStream2));
		outputStream1.execute();
		outputStream2.execute();
		Operator op;
		while(true){
			op = Scheduler.getInstance().getNextOperator();
			op.execute();
		}
//		System.out.println("end");
	}

}
