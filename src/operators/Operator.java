package operators;

import java.util.List;
import java.util.Queue;

import constants.Constants.OperatorState;

import json.Element;
import json.JsonSchema;
import jsonAPI.JsonQueryTree;


public abstract class Operator {
	private List<Queue<Element> > inputQueueList;
	private List<Queue<Element> > outputQueueList;
	private JsonSchema schema;
	
	private OperatorState state;
	private void setState(OperatorState state){
		this.state = state;
	}
	
	public Operator(JsonQueryTree tree){
		schema = tree.schema;
	}
	
	public abstract void execute();
	public OperatorState getState(){
		return state;
	}
	public void addInputQueue(Queue<Element> queue){
		inputQueueList.add(queue);
	}
	public void addOutputQueue(Queue<Element> queue){
		outputQueueList.add(queue);
	}
}
