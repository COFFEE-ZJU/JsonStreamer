package cn.edu.zju.jsonStreamer.operators;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.query.ProjectionDealer;
import cn.edu.zju.jsonStreamer.utils.ElementIdGenerator;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;


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
	protected void processPlus(MarkedElement markedElement) throws SystemErrorException{
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
			output(new MarkedElement(newEle, newId, ElementMark.PLUS, markedElement.timeStamp));
		}
		relatedMap.put(id, newIds);
	}
	
	@Override
	protected void processMinus(MarkedElement markedElement) throws SystemErrorException{
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
			output(new MarkedElement(eleToDelete, eleIdToDelete, ElementMark.MINUS, markedElement.timeStamp));
		}
	}

}
