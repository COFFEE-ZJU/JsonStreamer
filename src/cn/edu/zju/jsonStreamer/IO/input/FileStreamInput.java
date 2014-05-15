package cn.edu.zju.jsonStreamer.IO.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;

import com.google.gson.JsonElement;

public class FileStreamInput implements JStreamInput{
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
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String json;
			while((json = br.readLine()) != null){
				queue.add(json);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
