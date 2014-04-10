package operators;

import query.ProjectionDealer;

import json.Element;
import jsonAPI.JsonProjection;
import jsonAPI.JsonQueryTree;

public class ProjectionOperator extends Operator{
	private final JsonProjection projection;
	private final ProjectionDealer dealer;
	public ProjectionOperator(JsonQueryTree tree) {
		super(tree);
		projection = tree.projection;
		dealer = ProjectionDealer.genProjectionDealer(projection);
	}
	
	private Element project(Element ele){
		return dealer.deal(ele);
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
