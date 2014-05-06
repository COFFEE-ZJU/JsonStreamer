import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import scheduler.Scheduler;
import utils.SchemaServer;
import utils.StoppableThread;

import constants.Constants;

public class JsonStreamer extends StoppableThread{
	private int threadNum = 0;
	private ExecutorService threadPool;
	private ServerSocket ss;
	private Socket s;
	
	private static JsonStreamer jsonStreamer = null;
	private JsonStreamer(){};
	
	public static JsonStreamer getInstance(){
		if(jsonStreamer == null) jsonStreamer = new JsonStreamer();
		
		return jsonStreamer;
	}
	
	@Override
	protected void initialize() throws Exception {
		System.out.println("JsonStreamer starts serving");
		SchemaServer.getInstance().start();
		Scheduler.getInstance().start();
		
		threadPool = Executors.newFixedThreadPool(Constants.THREAD_POOL_NUM);
		ss = new ServerSocket(Constants.LISTEN_PORT);
		ss.setSoTimeout(Constants.SERVER_ACCEPT_TIMEOUT);
	}

	@Override
	protected void inLoop() throws Exception {
		try{
			s = ss.accept();
		} catch (SocketTimeoutException e) {
			return ;
		}
		threadPool.execute(new JsonStreamerThread(s,threadNum));
		synchronized ((Object)threadNum) {
			threadNum ++;
		}
	}

	@Override
	protected void finish() throws Exception{
		ss.close();
		SchemaServer.getInstance().stopTheThread();
		Scheduler.getInstance().stopTheThread();
		threadPool.awaitTermination(1, TimeUnit.SECONDS);
		threadPool.shutdown();
		System.out.println("JsonStreamer stops serving");
	}

	@Override
	protected void inException(Exception e) {
		System.err.println("failed to start JsonStreamer");
		e.printStackTrace();
		System.exit(1);
	}
	
	public static void main(String[] args) {
		JsonStreamer.getInstance().start();
		String cmd;
		Scanner scanner = new Scanner(System.in);
		while(true){
			cmd = scanner.nextLine();
			System.out.println(cmd);
			if(cmd.equals("QUIT")){
				JsonStreamer.getInstance().stopTheThread();
				break;
			}
		}
//		new SchemaServer().start();
//		String API1 = QueryParser.parseToAPIByFile("query_1");
//		String API2 = QueryParser.parseToAPIByFile("query_2");
//		System.out.println(API1);
//		System.out.println(API2);
//		
//		JStreamOutput outputStream2 = new ScreenStreamOutput();
//		JStreamOutput outputStream1 = new FileStreamOutput("output_1");
//		Scheduler.getInstance().addOperators(new APIParser().parse(API1, outputStream1));
//		Scheduler.getInstance().addOperators(new APIParser().parse(API2, outputStream2));
//		outputStream1.execute();
//		outputStream2.execute();
//		Operator op;
//		while(true){
//			op = Scheduler.getInstance().getNextOperator();
//			op.execute();
//		}
//		System.out.println("end");
	}

}
