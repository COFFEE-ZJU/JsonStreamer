package operators;

import java.util.HashMap;
import java.util.Map;

import query.ProjectionDealer;

import json.Element;
import json.ElementIdGenerator;
import jsonAPI.JsonProjection;
import jsonAPI.JsonQueryTree;

public class ProjectionOperator extends Operator{
	private final ProjectionDealer projDealer;
	private final Map<Long, Element> synopsis;
	private final Map<Long, Long> relatedMap;
	public ProjectionOperator(JsonQueryTree tree) {
		super(tree);
		projDealer = ProjectionDealer.genProjectionDealer(tree.projection);
		synopsis = new HashMap<Long, Element>();
		relatedMap = new HashMap<Long, Long>();
	}
	
	private void processPlus(long id, Element ele){
		Element newEle = projDealer.deal(ele);
		long newId = ElementIdGenerator.getNewId();
		synopsis.put(newId, newEle);
		//TODO put plus mark and output this element
		relatedMap.put(id, newId);
	}
	
	private void processMinus(long id){
		long eleIdToDelete = relatedMap.get(id);
		Element eleToDelete = synopsis.get(eleIdToDelete);
		//TODO put minus mark and output this element
		relatedMap.remove(id);
		synopsis.remove(eleIdToDelete);
	}

//	private JsonElement project(JsonElement ele, JsonProjection proj){
//		switch (proj.projection_type) {
//		case object:
//			JsonObject obj = new JsonObject();
//			Iterator<Entry<String, JsonProjection> > ito = proj.fields.entrySet().iterator();
//			Entry<String, JsonProjection> ent;
//			while(ito.hasNext()){
//				ent = ito.next();
//				obj.add(ent.getKey(), project(ele, ent.getValue()));
//			}
//			return obj;
//		case array:
//			JsonArray array = new JsonArray();
//			Iterator<JsonProjection> ita = proj.array_items.iterator();
//			while(ita.hasNext()){
//				array.add(project(ele, ita.next()));
//			}
//			return array;
//		case direct:
//			return new Element(ele).getValue(proj.expression);
//
//		default:
//			return null;
//		}
//	}
	
	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}
	

}
