package cn.edu.zju.jsonStreamer.IO.input;

import java.util.LinkedList;
import java.util.Queue;

import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.json.JsonSchema;
import cn.edu.zju.jsonStreamer.utils.RandomJsonGenerator;

import com.google.gson.JsonElement;


public class TestStreamInput implements JStreamInput{
	private boolean started = false;
	private Queue<String> queue;
	private RandomJsonGenerator generator;
	public TestStreamInput(JsonSchema schema){
		queue = new LinkedList<String>();
		generator = new RandomJsonGenerator(schema);
	}
	@Override
	public String getNextElement() {
		if(isEmpty()) return null;
		else{
			synchronized (queue) {
				return queue.poll();
			}
		}
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public void execute(){
		if(! started) new ExecThread().start();
	}
	
	class ExecThread extends Thread{
		@Override
		public void run(){
			for(int i=0;i<10;i++){
				synchronized (queue) {
					queue.add(Constants.gson.toJson(generator.generateRandomElement().jsonElement));
					try {
						Thread.sleep(RandomJsonGenerator.random.nextInt(100));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
