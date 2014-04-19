package operators;

import java.util.HashMap;
import java.util.Map;

import query.ConditionDealer;

import json.Element;
import json.MarkedElement;
import jsonAPI.JsonQueryTree;

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
