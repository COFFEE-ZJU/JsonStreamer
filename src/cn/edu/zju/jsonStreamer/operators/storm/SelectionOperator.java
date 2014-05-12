package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.HashSet;
import java.util.Set;

import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.query.ConditionDealer;


public class SelectionOperator extends OperatorOneInOneOut{
	private final ConditionDealer condDealer;
	private final Set<Long> synopsis;
	
	@Override
	protected void processPlus(MarkedElement markedElement){
		Long id = markedElement.id;
		Element ele = markedElement.element;
		if(condDealer.deal(ele)){
			synopsis.add(id);
			curCollector.emit(curTuple.getValues());
		}
	}
	
	@Override
	protected void processMinus(MarkedElement markedElement){
		Long id = markedElement.id;
		if(synopsis.contains(id)){
			synopsis.remove(id);
			curCollector.emit(curTuple.getValues());
		}
	}
	
	public SelectionOperator(JsonQueryTree tree){
		super(tree);
		condDealer = ConditionDealer.genConditionDealer(tree.selection_condition);
		synopsis = new HashSet<Long>();
	}
}
