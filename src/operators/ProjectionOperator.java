package operators;

import java.util.HashMap;
import java.util.Map;

import constants.Constants.ElementMark;

import query.ProjectionDealer;
import utils.ElementIdGenerator;

import json.Element;
import json.MarkedElement;
import jsonAPI.JsonQueryTree;

public class ProjectionOperator extends OperatorOneInOneOut{
	private final ProjectionDealer projDealer;
	private final Map<Long, Element> synopsis;
	private final Map<Long, Long> relatedMap;
	public ProjectionOperator(JsonQueryTree tree) {
		super(tree);
		projDealer = ProjectionDealer.genProjectionDealer(tree.projection);
		synopsis = new HashMap<Long, Element>();
		relatedMap = new HashMap<Long, Long>();
	}
	
	@Override
	protected void processPlus(MarkedElement markedElement){
		Long id = markedElement.id;
		Element ele = markedElement.element;
		Element newEle = projDealer.deal(ele);
		long newId = ElementIdGenerator.getNewId();
		synopsis.put(newId, newEle);
		outputQueue.add(new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp));
		relatedMap.put(id, newId);
	}
	
	@Override
	protected void processMinus(MarkedElement markedElement){
		Long id = markedElement.id;
		long eleIdToDelete = relatedMap.get(id);
		Element eleToDelete = synopsis.get(eleIdToDelete);
		outputQueue.add(new MarkedElement(eleToDelete, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp));
		relatedMap.remove(id);
		synopsis.remove(eleIdToDelete);
	}
}
