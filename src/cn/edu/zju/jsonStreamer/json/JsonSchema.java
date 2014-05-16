package cn.edu.zju.jsonStreamer.json;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.Constants.JsonValueType;

import com.google.gson.annotations.Expose;



public class JsonSchema implements Serializable{
	private JsonValueType valueType = null; 	//can be any type
	@Expose private String type = null; 	//can be any type
	@Expose public JsonSchema items = null;				//for array type only
	@Expose public Map<String, JsonSchema> properties = null;	//for object type only
	
	public JsonSchema(JsonValueType type){
		setType(type);
		if(getType() == JsonValueType.OBJECT) properties = new HashMap<String, JsonSchema>();
	}
	public JsonSchema(JsonSchema schema){
		schema.getType();
		setType(schema.valueType);
		if(schema.properties != null) this.properties = new HashMap<String, JsonSchema>(schema.properties);
		if(schema.items != null) this.items = new JsonSchema(schema.items);
	}
	
	public JsonValueType getType(){
		if(valueType == null) valueType = Constants.stringToJsonValueType.get(type);
		
		return valueType;
	}
	public void setType(JsonValueType type){
		valueType = type;
		this.type = valueType.toString().toLowerCase();
	}
	
	@Override
	public String toString(){
		return Constants.gson.toJson(this);
	}
	
	@Override
	public boolean equals(Object obj){
		if(!(obj instanceof JsonSchema)) return false;
		
		JsonSchema schema = (JsonSchema)obj;
		if((this.valueType == JsonValueType.INTEGER || this.valueType == JsonValueType.NUMBER) &&
			(schema.valueType == JsonValueType.INTEGER || schema.valueType == JsonValueType.NUMBER))
			return true;
		
		if(this.valueType != schema.valueType) return false;
		
		if(this.valueType == JsonValueType.ARRAY) return this.items.equals(schema.items);
		
		if(this.valueType == JsonValueType.OBJECT){
			if(this.properties.size() != schema.properties.size()) return false;
			Entry<String, JsonSchema> ent;
			Iterator<Entry<String, JsonSchema> > it = this.properties.entrySet().iterator();
			while(it.hasNext()){
				ent = it.next();
				if(! schema.properties.containsKey(ent.getKey())) return false;
				if(! ent.getValue().equals(schema.properties.get(ent.getKey()))) return false;
			}
			
			return true;
		}
		
		return true;
	}
	
	public JsonSchema getType(Object[] path){
		return getType(path, 0, this);
	}
	private JsonSchema getType(Object[] path, int index, JsonSchema schema){
		if(path.length == index) return schema;
		
		Object pathObj = path[index];
		if(pathObj.getClass() == String.class) return getType(path, index+1, schema.properties.get((String)pathObj));
		else if(pathObj.getClass() == Double.class && ((Double)pathObj).intValue() != -1) return getType(path, index+1, schema.items);
		else{
			JsonSchema retS = new JsonSchema(JsonValueType.ARRAY);
			retS.items = getType(path, index+1, schema.items);
			return retS;
		}
	}
}
