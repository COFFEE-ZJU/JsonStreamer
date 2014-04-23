package operators;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import constants.Constants.OperatorState;
import constants.SystemErrorException;

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
		inputQueueList = new LinkedList<Queue<MarkedElement> >();
		outputQueueList = new LinkedList<Queue<MarkedElement> >();
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
