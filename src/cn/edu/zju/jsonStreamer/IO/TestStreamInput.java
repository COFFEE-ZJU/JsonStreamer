package cn.edu.zju.jsonStreamer.IO;

import java.util.LinkedList;
import java.util.Queue;

import com.google.gson.JsonElement;

import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.JsonSchema;
import cn.edu.zju.jsonStreamer.utils.RandomJsonGenerator;


public class TestStreamInput implements JStreamInput{
	private boolean started = false;
	private Queue<JsonElement> queue;
	private RandomJsonGenerator generator;
	public TestStreamInput(JsonSchema schema){
		queue = new LinkedList<JsonElement>();
		generator = new RandomJsonGenerator(schema);
	}
	@Override
	public JsonElement getNextElement() {
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
					queue.add(generator.generateRandomElement().jsonElement);
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
