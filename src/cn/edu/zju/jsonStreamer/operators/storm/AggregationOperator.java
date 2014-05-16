package cn.edu.zju.jsonStreamer.operators.storm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.Constants.JsonValueType;
import cn.edu.zju.jsonStreamer.constants.Constants.StormFields;
import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonProjection;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.query.ConditionDealer;
import cn.edu.zju.jsonStreamer.query.ProjectionDealer;
import cn.edu.zju.jsonStreamer.utils.ElementIdGenerator;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;


public class AggregationOperator extends OperatorOneInOneOut{
	private Map<Integer, JsonArray> groupMap;		//groupKey's hashCode to the Element array
//	private final Map<Long, Element> synopsis;			//output Element Id to the Element
	private Map<Integer, Long> relatedMap;		//groupKey's hashCode to output Element Id
//	private final Map<Long, Integer> idToGroupKey;		//input Element Id to groupKey's hashCode
	private ProjectionDealer projDealer;
	private final JsonProjection proj;
	private Map<Long, JsonElement> inputMapper;		//input element id to Json Element
//	private final ExpressionDealer groupAttrDealer;
	
	public AggregationOperator(JsonQueryTree tree) {
		super(tree);
		proj = tree.projection;
		
	}
	
	@Override
    public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		groupMap = new HashMap<Integer, JsonArray>();
		projDealer = ProjectionDealer.genProjectionDealer(proj);
//		groupAttrDealer = ExpressionDealer.genExpressionDealer(tree.groupby_attribute_name);
//		synopsis = new HashMap<Long, Element>();
		relatedMap = new HashMap<Integer, Long>();
		inputMapper = new HashMap<Long, JsonElement>();
//		idToGroupKey = new HashMap<Long, Integer>();
    }
	
	@Override
	protected void processPlus(MarkedElement markedElement){
		Long id = markedElement.id;
		Element ele = markedElement.element;
		inputMapper.put(id, ele.jsonElement);
		Element groupByEle = gson.fromJson(curTuple.getStringByField(StormFields.groupKey), Element.class);
		int groupByKey = groupByEle.hashCode();
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
//			Element eleToDelete = synopsis.get(eleIdToDelete);
//			synopsis.remove(eleIdToDelete);
			curCollector.emit(new Values(eleIdToDelete, 
					gson.toJson(new MarkedElement(null, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp))));
//			outputQueue.add(new MarkedElement(eleToDelete, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp));
		}
		Element arrayToDeal = new Element(groupMap.get(groupByKey), JsonValueType.ARRAY);
		Element newEle = projDealer.deal(arrayToDeal, groupByEle);
		long newId = ElementIdGenerator.getNewId();
		curCollector.emit(new Values(newId, 
				gson.toJson(new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp))));
//		outputQueue.add(new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp));
		
//		synopsis.put(newId, newEle);
		relatedMap.put(groupByKey, newId);
//		idToGroupKey.put(id, groupByKey);
	}
	
	@Override
	protected void processMinus(MarkedElement markedElement){
		Long id = markedElement.id;
		Element ele = markedElement.element;
		Element groupByEle = gson.fromJson(curTuple.getStringByField(StormFields.groupKey), Element.class);
		int groupByKey = groupByEle.hashCode();
		JsonArray array = groupMap.get(groupByKey);
		JsonArray newA = removeElementInJsonArray(array, inputMapper.get(id));
		inputMapper.remove(id);
		
		long eleIdToDelete = relatedMap.get(groupByKey);
//		Element eleToDelete = synopsis.get(eleIdToDelete);
//		synopsis.remove(eleIdToDelete);
		curCollector.emit(new Values(eleIdToDelete, 
				gson.toJson(new MarkedElement(null, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp))));
//		outputQueue.add(new MarkedElement(eleToDelete, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp));
		
		if(newA.size() != 0){
			groupMap.put(groupByKey, removeElementInJsonArray(array, ele.jsonElement));
			Element arrayToDeal = new Element(groupMap.get(groupByKey), JsonValueType.ARRAY);
			Element newEle = projDealer.deal(arrayToDeal, groupByEle);
			long newId = ElementIdGenerator.getNewId();
			curCollector.emit(new Values(newId, 
					gson.toJson(new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp))));
//			outputQueue.add(new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp));
//			synopsis.put(newId, newEle);
			relatedMap.put(groupByKey, newId);
		}
		else{
			groupMap.remove(groupByKey);
			relatedMap.remove(groupByKey);
		}
//		idToGroupKey.remove(id);
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
