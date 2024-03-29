package IO;

import java.util.LinkedList;
import java.util.Queue;

import utils.RandomJsonGenerator;

import json.Element;
import json.JsonSchema;

public class TestStreamInput implements JStreamInput{
	private boolean started = false;
	private Queue<Element> queue;
	private RandomJsonGenerator generator;
	public TestStreamInput(JsonSchema schema){
		queue = new LinkedList<Element>();
		generator = new RandomJsonGenerator(schema);
	}
	@Override
	public Element getNextElement() {
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
					queue.add(generator.generateRandomElement());
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
