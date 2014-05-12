package cn.edu.zju.jsonStreamer.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonProjection;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public abstract class ProjectionDealer {
	protected final JsonProjection projection;
	protected ProjectionDealer(JsonProjection proj){
		projection = proj;
	}
	
	public Element deal(Element ele){
		return deal(ele, null);
	}
	public abstract Element deal(Element ele, Element rightEle);
	
	public static ProjectionDealer genProjectionDealer(JsonProjection proj){
		switch (proj.projection_type) {
		case ARRAY:
			return new ArrayDealer(proj);
		case DIRECT:
			return new DirectDealer(proj);
		case OBJECT:
			return new ObjectDealer(proj);
		}
		return null;
	}
}

class JoinDealer {
	public JoinDealer(JsonProjection proj){
		
	}
}

class DirectDealer extends ProjectionDealer{
	private final ExpressionDealer exprDealer;
	protected DirectDealer(JsonProjection proj) {
		super(proj);
		exprDealer = ExpressionDealer.genExpressionDealer(proj.expression);
	}

	@Override
	public Element deal(Element ele, Element rightEle) {
		return exprDealer.deal(ele, rightEle);
	}
}

class ObjectDealer extends ProjectionDealer{
	private final Map<String, ProjectionDealer> map;
	protected ObjectDealer(JsonProjection proj) {
		super(proj);
		map = new HashMap<String, ProjectionDealer>();
		Iterator<Entry<String, JsonProjection> > it = projection.fields.entrySet().iterator();
		Entry<String, JsonProjection> ent;
		while(it.hasNext()){
			ent = it.next();
			map.put(ent.getKey(), ProjectionDealer.genProjectionDealer(ent.getValue()));
		}
	}

	@Override
	public Element deal(Element ele, Element rightEle) {
		JsonObject obj = new JsonObject();
		Iterator<Entry<String, ProjectionDealer> > it = map.entrySet().iterator();
		Entry<String, ProjectionDealer> ent;
		while(it.hasNext()){
			ent = it.next();
			obj.add(ent.getKey(), ent.getValue().deal(ele, rightEle).jsonElement);
		}
		return new Element(obj, projection.retSchema.getType());
	}
}

class ArrayDealer extends ProjectionDealer{
	private final List<ProjectionDealer> list;
	protected ArrayDealer(JsonProjection proj) {
		super(proj);
		list = new ArrayList<ProjectionDealer>();
		Iterator<JsonProjection> it = proj.array_items.iterator();
		while(it.hasNext()) list.add(ProjectionDealer.genProjectionDealer(it.next()));
	}

	@Override
	public Element deal(Element ele, Element rightEle) {
		JsonArray array = new JsonArray();
		Iterator<ProjectionDealer> it = list.iterator();
		while(it.hasNext()) array.add(it.next().deal(ele, rightEle).jsonElement);
		return new Element(array, projection.retSchema.getType());
	}
	
}

