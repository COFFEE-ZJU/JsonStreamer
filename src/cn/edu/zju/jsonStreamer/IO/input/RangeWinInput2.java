package cn.edu.zju.jsonStreamer.IO.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import cn.edu.zju.jsonStreamer.constants.SystemErrorException;

public class RangeWinInput2 implements JStreamInput{
	private boolean started = false;
	private File file;
	private Queue<String> queue;
	public RangeWinInput2(){
		file = new File("inputFiles/rangeWinInput2.txt");
		queue = new LinkedList<String>();
	}
	
	@Override
	public String getNextElement() throws SystemErrorException {
		if(isEmpty()) return null;
		return queue.poll();
	}

	@Override
	public boolean isEmpty() {
		return queue.isEmpty();
	}

	@Override
	public void execute() {
		if(! started) new ExecThread().start();
		started = true;
	}
	
	class ExecThread extends Thread{
		private int[] sleeps = new int[]{2,4};
		private int cnt = 0;
		@Override
		public void run(){
			try {
				BufferedReader br = new BufferedReader(new FileReader(file));
				String json;
				while((json = br.readLine()) != null){
					if(cnt<sleeps.length){
						try {
							Thread.sleep(sleeps[cnt]*1000);
						} catch (InterruptedException e) {}
						queue.add(json);
						cnt++;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
