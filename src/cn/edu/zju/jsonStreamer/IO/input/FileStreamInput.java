package cn.edu.zju.jsonStreamer.IO.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import cn.edu.zju.jsonStreamer.IO.input.TestStreamInput.ExecThread;
import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.utils.RandomJsonGenerator;

import com.google.gson.JsonElement;

public class FileStreamInput implements JStreamInput{
	private boolean started = false;
	private File file;
	private Queue<String> queue;
	public FileStreamInput(String filePath){
		file = new File(filePath);
		queue = new LinkedList<String>();
	}
	
	@Override
	public String getNextElement() throws SystemErrorException {
		if(isEmpty()) return null;
		try {
			Thread.sleep(2);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		@Override
		public void run(){
			try {
				BufferedReader br = new BufferedReader(new FileReader(file));
				String json;
				while((json = br.readLine()) != null){
					queue.add(json);
					try {
						Thread.sleep(RandomJsonGenerator.random.nextInt(1000));
					} catch (InterruptedException e) {}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
