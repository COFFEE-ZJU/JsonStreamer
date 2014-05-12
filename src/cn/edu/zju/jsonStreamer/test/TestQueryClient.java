package cn.edu.zju.jsonStreamer.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Scanner;

import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.utils.StoppableThread;


public class TestQueryClient extends StoppableThread{
	public static final int SEND_PORT = cn.edu.zju.jsonStreamer.constants.Constants.LISTEN_PORT;
	private String queryFileName;
	private InputStream in=null;
	private OutputStream out=null;
	private BufferedReader br=null;
	private Socket socket=null;
	public TestQueryClient(String query){
		queryFileName = query;
	}
	public InputStream getInput(){
		return in;
	}
	
	public void writeCmd(String cmd) throws IOException{
		out.write(cmd.getBytes());
		out.write("\n".getBytes());
		out.flush();
	}

	@Override
	protected void initialize() throws Exception {
		socket = new Socket("localhost", SEND_PORT);
		in = socket.getInputStream();
		out = socket.getOutputStream();
		BufferedReader reader = new BufferedReader(
				new FileReader(Constants.QUERY_FILE_PATH + queryFileName + ".txt"));
		StringBuffer outString = new StringBuffer();
		String tmp;
		while((tmp = reader.readLine()) != null){
			outString.append(tmp+"\r\n");
		}
		out.write(outString.toString().getBytes());
		out.flush();
		br = new BufferedReader(new InputStreamReader(in));
	}
	@Override
	protected void inLoop() throws Exception {
		String line = br.readLine();
		if(line == null)
			stopTheThread();
		else System.out.println(line);
	}
	@Override
	protected void finish() throws Exception {
		br.close();
		in.close();
		out.close();
		socket.close();
		System.out.println("client closed");
	}
	@Override
	protected void inException(Exception e) {
	}

	public static void main(String[] args) throws IOException {
		TestQueryClient tqc = new TestQueryClient("query_1");
		tqc.start();
		Scanner scanner = new Scanner(System.in);
		String cmd;
		while(true){
			cmd = scanner.nextLine();
			tqc.writeCmd(cmd);
			if(cmd.equals("QUIT")) break;
		}
	}
}
