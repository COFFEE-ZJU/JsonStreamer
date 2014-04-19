package operators;

import java.util.Queue;

import constants.SystemErrorException;
import constants.Constants.ElementMark;

import json.MarkedElement;
import jsonAPI.JsonQueryTree;

public abstract class OperatorOneInOneOut extends Operator{
	public OperatorOneInOneOut(JsonQueryTree tree) {
		super(tree);
	}
	protected Queue<MarkedElement> inputQueue = null;
	protected Queue<MarkedElement> outputQueue = null;
	
	protected abstract void processMinus(MarkedElement markedElement);
	protected abstract void processPlus(MarkedElement markedElement);
	
	@Override
	public final void execute() {
		if(inputQueue == null || outputQueue == null){
			if(inputQueueList.size() != 1 || outputQueueList.size() != 1) throw new SystemErrorException("queue size abnormal");
			inputQueue = inputQueueList.get(0);
			outputQueue = outputQueueList.get(0);
		}
		MarkedElement me;
		while(! inputQueue.isEmpty()){
			me = inputQueue.poll();
			if(me.mark == ElementMark.MINUS) processMinus(me);
			else processPlus(me);
		}
	}
	
}
