package cn.edu.zju.jsonStreamer.operators;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import cn.edu.zju.jsonStreamer.constants.Constants.OperatorState;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.JsonSchema;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;



public abstract class Operator {
	protected List<Queue<MarkedElement> > inputQueueList;
	protected List<Queue<MarkedElement> > outputQueueList;
	protected JsonSchema schema;
	
	private OperatorState state;
	private void setState(OperatorState state){
		this.state = state;
	}
	
	public Operator(JsonQueryTree tree){
		schema = tree.schema;
		inputQueueList = new LinkedList<Queue<MarkedElement> >();
		outputQueueList = new LinkedList<Queue<MarkedElement> >();
	}
	
	protected final void output(MarkedElement me) throws SystemErrorException{
		if(outputQueueList.isEmpty()) throw new SystemErrorException("empty output list");
		for(Queue<MarkedElement> queue: outputQueueList){
			queue.add(me);
		}
	}
	
	public abstract void execute() throws SystemErrorException;
	public OperatorState getState(){
		return state;
	}
	public void addInputQueue(Queue<MarkedElement> queue){
		inputQueueList.add(queue);
	}
	public void addOutputQueue(Queue<MarkedElement> queue){
		outputQueueList.add(queue);
	}
}
