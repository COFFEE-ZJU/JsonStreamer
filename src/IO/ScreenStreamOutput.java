package IO;

import java.util.LinkedList;
import java.util.Queue;

import constants.SystemErrorException;

import json.Element;
import json.MarkedElement;

public class ScreenStreamOutput implements JStreamOutput{
	private boolean started = false;
	private Queue<Element> queue = new LinkedList<Element>();
	@Override
	public boolean pushNext(MarkedElement ele) {
		if(ele.element == null) throw new SystemErrorException("null found");
		synchronized (queue) {
			queue.add(ele.element);
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
					Element ele = queue.poll();
					if(ele == null) throw new SystemErrorException("null found");
					System.out.println(ele);
				}
			}
		}
	}
}
