package cn.edu.zju.jsonStreamer.operators;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.parse.RegisterForLocal;


public abstract class OperatorStreamToRelation extends Operator{
	protected Queue<MarkedElement> inputQueue = null;
//	protected Queue<MarkedElement> outputQueue = null;
	
	private int opNum;
	protected RegisterForLocal register;
	protected boolean doneFlush;
	protected int bufferedNum = 0;
	
	protected final Queue<MarkedElement> synopsis;
	
	public OperatorStreamToRelation(JsonQueryTree tree, RegisterForLocal register, int opNum) {
		super(tree);
		synopsis = new LinkedList<MarkedElement>();
		this.opNum = opNum;
		this.register = register;
	}
	
	
	protected abstract void process(MarkedElement markedElement) throws SystemErrorException;
	
	protected void flushElements() throws SystemErrorException{
		Iterator<MarkedElement> it = synopsis.iterator();
		while(bufferedNum > 0){
			output(it.next());
			bufferedNum --;
		}
	}
	
	@Override
	public final void execute() throws SystemErrorException {
		if(inputQueue == null){
			if(inputQueueList.size() != 1) throw new SystemErrorException("queue size abnormal");
			inputQueue = inputQueueList.get(0);
//			outputQueue = outputQueueList.get(0);
		}
		MarkedElement me;
		while(! inputQueue.isEmpty()){
			me = inputQueue.poll();
			if(me.mark == ElementMark.MINUS)
				throw new SystemErrorException("stream element can't be MINUS marked");
			else process(me);
		}
		
		if(register.needFlush()){
			flushElements();
			register.windowDoneFlush(opNum);
		}
	}

}
