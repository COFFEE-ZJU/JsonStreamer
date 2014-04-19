package operators;

import java.util.List;
import java.util.Queue;

import constants.Constants.OperatorState;

import json.Element;
import json.JsonSchema;
import json.MarkedElement;
import jsonAPI.JsonQueryTree;


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
	}
	
	public abstract void execute();
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
