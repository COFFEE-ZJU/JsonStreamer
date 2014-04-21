package operators;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import constants.Constants.ElementMark;

import query.ProjectionDealer;
import utils.ElementIdGenerator;

import json.Element;
import json.MarkedElement;
import jsonAPI.JsonQueryTree;

public class ExpandOperator extends OperatorOneInOneOut{
	private final ProjectionDealer projDealer;
	private final Map<Long, Element> synopsis;
	private final Map<Long, Set<Long> > relatedMap;
	
	public ExpandOperator(JsonQueryTree tree) {
		super(tree);
		projDealer = ProjectionDealer.genProjectionDealer(tree.projection);
		synopsis = new HashMap<Long, Element>();
		relatedMap = new HashMap<Long, Set<Long>>();
	}
	
	@Override
	protected void processPlus(MarkedElement markedElement){
		Long id = markedElement.id;
		Element ele = markedElement.element;
		JsonArray arrayEle = projDealer.deal(ele).jsonElement.getAsJsonArray();
		Set<Long> newIds = new HashSet<Long>();
		Iterator<JsonElement> it = arrayEle.iterator();
		Element newEle;
		Long newId;
		while(it.hasNext()){
			newEle = new Element(it.next(), schema.getType());
			newId = ElementIdGenerator.getNewId();
			newIds.add(newId);
			synopsis.put(newId, newEle);
			outputQueue.add(new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp));
		}
		relatedMap.put(id, newIds);
	}
	
	@Override
	protected void processMinus(MarkedElement markedElement){
		Long id = markedElement.id;
		Set<Long> eleIdsToDelete = relatedMap.get(id);
		relatedMap.remove(id);
		Iterator<Long> it = eleIdsToDelete.iterator();
		Long eleIdToDelete;
		Element eleToDelete;
		while(it.hasNext()){
			eleIdToDelete = it.next();
			eleToDelete = synopsis.get(eleIdToDelete);
			synopsis.remove(eleIdToDelete);
			outputQueue.add(new MarkedElement(eleToDelete, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp));
		}
	}

}
