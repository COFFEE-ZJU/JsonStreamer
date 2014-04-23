package IO;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.Queue;

import constants.Constants;
import constants.SystemErrorException;

import json.Element;
import json.MarkedElement;

public class FileStreamOutput implements JStreamOutput{
	private boolean started = false;
	private String fileName;
	private Queue<Element> queue = new LinkedList<Element>();
	
	public FileStreamOutput(String fileName){
		this.fileName = fileName;
	}
	
	@Override
	public boolean pushNext(MarkedElement ele) throws SystemErrorException{
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
	public void execute() {
		if(! started) new ExecThread().start();
	}
	
	class ExecThread extends Thread{
		@Override
		public void run(){
			File file = new File(Constants.RESULT_FILE_PATH+fileName+".txt");
            PrintStream ps;
			try {
				ps = new PrintStream(new FileOutputStream(file));
				ps.print("");
				while(true){
					synchronized (queue) {
						if(queue.isEmpty()) continue;
						Element ele = queue.poll();
						if(ele == null) throw new SystemErrorException("null found");
						ps.append(ele.toString()+"\r\n");
					}
				}
			} catch (FileNotFoundException | SystemErrorException e) {
				e.printStackTrace();
			}
		}
	}
}
