package operators;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import query.ExpressionDealer;
import query.ProjectionDealer;
import utils.ElementIdGenerator;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import constants.Constants.ElementMark;
import constants.Constants.JsonValueType;

import json.Element;
import json.MarkedElement;
import jsonAPI.JsonQueryTree;

public class AggregationOperator extends OperatorOneInOneOut{
	private final Map<Integer, JsonArray> groupMap;		//groupKey's hashCode to the Element array
	private final Map<Long, Element> synopsis;			//output Element Id to the Element
	private final Map<Integer, Long> relatedMap;		//groupKey's hashCode to output Element Id
	private final Map<Long, Integer> idToGroupKey;		//input Element Id to groupKey's hashCode
	private final ProjectionDealer projDealer;
	private final ExpressionDealer groupAttrDealer;
	
	public AggregationOperator(JsonQueryTree tree) {
		super(tree);
		groupMap = new HashMap<Integer, JsonArray>();
		projDealer = ProjectionDealer.genProjectionDealer(tree.projection);
		groupAttrDealer = ExpressionDealer.genExpressionDealer(tree.groupby_attribute_name);
		synopsis = new HashMap<Long, Element>();
		relatedMap = new HashMap<Integer, Long>();
		idToGroupKey = new HashMap<Long, Integer>();
	}
	
	@Override
	protected void processPlus(MarkedElement markedElement){
		Long id = markedElement.id;
		Element ele = markedElement.element;
		int groupByKey = groupAttrDealer.deal(ele).hashCode();
		JsonArray array;
		if(! groupMap.containsKey(groupByKey)){
			array = new JsonArray();
			array.add(ele.jsonElement);
			groupMap.put(groupByKey, array);
		}
		else{
			array = groupMap.get(groupByKey);
			array.add(ele.jsonElement);
			long eleIdToDelete = relatedMap.get(groupByKey);
			Element eleToDelete = synopsis.get(eleIdToDelete);
			synopsis.remove(eleIdToDelete);
			outputQueue.add(new MarkedElement(eleToDelete, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp));
		}
		Element arrayToDeal = new Element(groupMap.get(groupByKey), JsonValueType.ARRAY);
		Element newEle = projDealer.deal(arrayToDeal);
		long newId = ElementIdGenerator.getNewId();
		outputQueue.add(new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp));
		
		synopsis.put(newId, newEle);
		relatedMap.put(groupByKey, newId);
		idToGroupKey.put(id, groupByKey);
	}
	
	@Override
	protected void processMinus(MarkedElement markedElement){
		Long id = markedElement.id;
		Element ele = markedElement.element;
		int groupByKey = idToGroupKey.get(id);
		JsonArray array = groupMap.get(groupByKey);
		JsonArray newA = removeElementInJsonArray(array, ele.jsonElement);
		
		long eleIdToDelete = relatedMap.get(groupByKey);
		Element eleToDelete = synopsis.get(eleIdToDelete);
		synopsis.remove(eleIdToDelete);
		outputQueue.add(new MarkedElement(eleToDelete, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp));
		
		if(newA.size() != 0){
			groupMap.put(groupByKey, removeElementInJsonArray(array, ele.jsonElement));
			Element arrayToDeal = new Element(groupMap.get(groupByKey), JsonValueType.ARRAY);
			Element newEle = projDealer.deal(arrayToDeal);
			long newId = ElementIdGenerator.getNewId();
			outputQueue.add(new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp));
			synopsis.put(newId, newEle);
			relatedMap.put(groupByKey, newId);
		}
		else{
			groupMap.remove(groupByKey);
			relatedMap.remove(groupByKey);
		}
		
		idToGroupKey.remove(id);
		
	}
	
	
	private JsonArray removeElementInJsonArray(JsonArray array, JsonElement ele){
		JsonArray retA = new JsonArray();
		Iterator<JsonElement> it = array.iterator();
		JsonElement element;
		while(it.hasNext()){
			element = it.next();
			if(! ele.equals(element)) retA.add(element);
		}
		
		return retA;
	}

}
