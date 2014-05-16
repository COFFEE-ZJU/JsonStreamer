package cn.edu.zju.jsonStreamer.operators;

import java.util.Iterator;
import java.util.Queue;

import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;


public abstract class OperatorOneInOneOut extends Operator{
	public OperatorOneInOneOut(JsonQueryTree tree) {
		super(tree);
	}
	protected Queue<MarkedElement> inputQueue = null;
//	protected Queue<MarkedElement> outputQueue = null;
	
	protected abstract void processMinus(MarkedElement markedElement) throws SystemErrorException;
	protected abstract void processPlus(MarkedElement markedElement) throws SystemErrorException;
	
	@Override
	public final void execute() throws SystemErrorException {
		if(inputQueue == null){
			if(inputQueueList.size() != 1)
				throw new SystemErrorException("input queue size abnormal");
			inputQueue = inputQueueList.get(0);
		}
		MarkedElement me;
		while(! inputQueue.isEmpty()){
			me = inputQueue.poll();
			if(me.mark == ElementMark.MINUS) processMinus(me);
			else processPlus(me);
		}
	}
	
}
