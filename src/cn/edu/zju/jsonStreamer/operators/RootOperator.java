package cn.edu.zju.jsonStreamer.operators;

import java.util.Queue;

import cn.edu.zju.jsonStreamer.IO.JStreamOutput;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;


public class RootOperator extends Operator{
	protected final JStreamOutput outputStream;
	protected Queue<MarkedElement> inputQueue = null;
	
	public RootOperator(JsonQueryTree tree, JStreamOutput outputStream) {
		super(tree);
		this.outputStream = outputStream;
	}
	
	@Override
	public final void execute() throws SystemErrorException{
		if(inputQueue == null){
			if(inputQueueList.size() != 1)
				throw new SystemErrorException("queue size abnormal");
			inputQueue = inputQueueList.get(0);
		}
		while(! inputQueue.isEmpty()){
			outputStream.pushNext(inputQueue.poll());
		}
	}

}
