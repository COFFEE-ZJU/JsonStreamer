package operators;

import java.util.Queue;

import constants.SystemErrorException;
import IO.JStreamOutput;
import json.MarkedElement;
import jsonAPI.JsonQueryTree;

public abstract class OperatorRelationToStream extends Operator{
	protected final JStreamOutput outputStream;
	protected Queue<MarkedElement> inputQueue = null;
	
	public OperatorRelationToStream(JsonQueryTree tree, JStreamOutput outputStream) {
		super(tree);
		this.outputStream = outputStream;
	}
	
	protected abstract void process(MarkedElement markedElement);
	
	@Override
	public final void execute(){
		if(inputQueue == null){
			if(inputQueueList.size() != 1)
				throw new SystemErrorException("queue size abnormal");
			inputQueue = inputQueueList.get(0);
		}
		MarkedElement markedElement;
		while(! inputQueue.isEmpty()){
			markedElement = inputQueue.poll();
			process(markedElement);
		}
	}

}
