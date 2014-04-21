package operators;

import java.util.Queue;

import com.google.gson.JsonPrimitive;

import utils.ElementIdGenerator;
import utils.JsonSchemaUtils;
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
		inputStream.execute();
		isMaster = tree.is_master;
	}
	
	@Override
	public void execute() {
		if(outputQueue == null){
			if(outputQueueList.size() != 1){
				System.err.println(outputQueueList.size());
				throw new SystemErrorException("queue size abnormal");
			}
			outputQueue = outputQueueList.get(0);
		}
		Element ele;
		while(! inputStream.isEmpty()){
			ele = inputStream.getNextElement();
			if(ele == null) return ;
			if(! JsonSchemaUtils.validate(ele.jsonElement, schema)) continue ;
			outputQueue.add(new MarkedElement(ele, 
					ElementIdGenerator.getNewId(), 
					ElementMark.PLUS, 
					TimeStampGenerator.getCurrentTimeStamp()));
		}
	}

}
