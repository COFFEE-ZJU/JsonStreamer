package operators;

import java.util.Queue;

import utils.ElementIdGenerator;
import utils.TimeStampGenerator;

import constants.Constants.ElementMark;
import constants.SystemErrorException;

import json.Element;
import json.MarkedElement;
import jsonAPI.JsonQueryTree;
import IO.IOManager;
import IO.JStreamInput;

public class LeafOperator extends Operator{
	private boolean isMaster = false;
	private JStreamInput inputStream;
	private Queue<MarkedElement> outputQueue = null;
	
	public LeafOperator(JsonQueryTree tree){
		super(tree);
		inputStream = IOManager.getInstance().getInputStreamByName(tree.stream_source);
		isMaster = tree.is_master;
	}
	
	@Override
	public void execute() {
		if(outputQueue == null){
			if(outputQueueList.size() != 1)
				throw new SystemErrorException("queue size abnormal");
			outputQueue = outputQueueList.get(0);
		}
		Element ele;
		while(! inputStream.isEmpty()){
			ele = inputStream.getNextElement();
			if(ele == null) return ;
			outputQueue.add(new MarkedElement(ele, 
					ElementIdGenerator.getNewId(), 
					ElementMark.PLUS, 
					TimeStampGenerator.getCurrentTimeStamp()));
		}
	}

}
