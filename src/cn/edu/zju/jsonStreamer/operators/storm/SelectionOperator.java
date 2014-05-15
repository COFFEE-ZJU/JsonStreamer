package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.TopologyContext;

import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonCondition;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.query.ConditionDealer;


public class SelectionOperator extends OperatorOneInOneOut{
	private ConditionDealer condDealer;
	private Set<Long> synopsis;
	private final JsonCondition selectCond;
	public SelectionOperator(JsonQueryTree tree){
		super(tree);
		selectCond = tree.selection_condition;
	}
	
	@Override
    public void prepare(Map stormConf, TopologyContext context) {
		condDealer = ConditionDealer.genConditionDealer(selectCond);
		synopsis = new HashSet<Long>();
    }
	
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
	
}
