package cn.edu.zju.jsonStreamer.operators;

import java.util.LinkedList;
import java.util.Queue;

import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;


public abstract class OperatorStreamToRelation extends Operator{
	protected Queue<MarkedElement> inputQueue = null;
	protected Queue<MarkedElement> outputQueue = null;
	
	protected final Queue<MarkedElement> synopsis;
	
	public OperatorStreamToRelation(JsonQueryTree tree) {
		super(tree);
		synopsis = new LinkedList<MarkedElement>();
	}
	
	protected abstract void process(MarkedElement markedElement);
	
	@Override
	public final void execute() throws SystemErrorException {
		if(inputQueue == null || outputQueue == null){
			if(inputQueueList.size() != 1 || outputQueueList.size() != 1) throw new SystemErrorException("queue size abnormal");
			inputQueue = inputQueueList.get(0);
			outputQueue = outputQueueList.get(0);
		}
		MarkedElement me;
		while(! inputQueue.isEmpty()){
			me = inputQueue.poll();
			if(me.mark == ElementMark.MINUS)
				throw new SystemErrorException("stream element can't be MINUS marked");
			else process(me);
		}
	}

}
