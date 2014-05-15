package cn.edu.zju.jsonStreamer.operators;

import java.util.HashMap;
import java.util.Map;

import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.query.ConditionDealer;


public class SelectionOperator extends OperatorOneInOneOut{
	private final ConditionDealer condDealer;
	
	private final Map<Long, Element> synopsis;
	
	@Override
	protected void processPlus(MarkedElement markedElement){
		Long id = markedElement.id;
		Element ele = markedElement.element;
		if(condDealer.deal(ele)){
			synopsis.put(id, ele);
			outputQueue.add(markedElement);
		}
	}
	
	@Override
	protected void processMinus(MarkedElement markedElement){
		Long id = markedElement.id;
		if(synopsis.containsKey(id)){
			synopsis.remove(id);
			outputQueue.add(markedElement);
		}
	}
	
	public SelectionOperator(JsonQueryTree tree){
		super(tree);
		condDealer = ConditionDealer.genConditionDealer(tree.selection_condition);
		synopsis = new HashMap<Long, Element>();
	}
}