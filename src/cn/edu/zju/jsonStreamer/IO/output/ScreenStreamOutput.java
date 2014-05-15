package cn.edu.zju.jsonStreamer.IO.output;

import java.util.LinkedList;
import java.util.Queue;

import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.MarkedElement;


public class ScreenStreamOutput implements JStreamOutput{
	private boolean started = false;
	private Queue<String> queue = new LinkedList<String>();
	@Override
	public boolean pushNext(String ele) throws SystemErrorException {
		if(ele == null) throw new SystemErrorException("null found");
		synchronized (queue) {
			queue.add(ele);
		}
		return true;
	}

	@Override
	public boolean isFull() {
		return false;
	}
	
	@Override
	public void execute(){		//for thread
		if(! started) new ExecThread().start();
	}
	
	class ExecThread extends Thread{
		@Override
		public void run(){
			while(true){
				synchronized (queue) {
					if(queue.isEmpty()) continue;
					String ele = queue.poll();
					System.out.println(ele);
				}
			}
		}
	}
}
