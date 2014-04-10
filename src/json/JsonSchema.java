package json;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;

import constants.Constants;
import constants.Constants.*;


public class JsonSchema {
	static class JsonSchemaQuery{
		String type;
		String wrapper_name;
		
		public JsonSchemaQuery(String wrapperName){
			type = "get_schema";
			wrapper_name = wrapperName;
		}
	}
	
	static class SchemaFormat{
		JsonValueType type;
		Object properties;
		SchemaFormat items;
	}
	
	@Expose public JsonValueType type = null; 	//can be any type
	@Expose public JsonSchema items = null;				//for array type only
	@Expose public Map<String, JsonSchema> nameToSchema = new HashMap<String, JsonSchema>();	//for object type only
	
//	Map<String, JsonValueType> nameToType = new HashMap<String, JsonValueType>();
//	Map<String, JsonSchema> objectNameToSchema = new HashMap<String, JsonSchema>();
//	Map<String, JsonValueType> arrayNameToType = new HashMap<String, JsonValueType>();
	
	public JsonSchema(){}
	public JsonSchema(JsonValueType type){
		this.type = type;
	}
	public JsonSchema(JsonSchema schema){
		this.type = schema.type;
		this.nameToSchema = new HashMap<String, JsonSchema>(schema.nameToSchema);
		if(schema.items != null) this.items = new JsonSchema(schema.items);
		
//		this.nameToType = new HashMap<String, JsonValueType>(schema.nameToType);
//		this.objectNameToSchema = new HashMap<String, JsonSchema>(schema.objectNameToSchema);
//		this.arrayNameToType = new HashMap<String, JsonValueType>(schema.arrayNameToType);
	}
	
	
	public static JsonSchema getSchema(String wrapperName){

		File file = new File(Constants.schemaFilePath + wrapperName + ".txt");
		Long fileLengthLong = file.length();
		byte[] fileContent = new byte[fileLengthLong.intValue()];
		try {
			FileInputStream inputStream = new FileInputStream(file);
			inputStream.read(fileContent);
			inputStream.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
		String jsonString = new String(fileContent);
		
		Object o = Constants.gson.fromJson(jsonString, Object.class);
		
		//System.out.println(parseSchema(toSchemaFormat(o)));
		return parseSchema(toSchemaFormat(o));
	}
	
	private static JsonSchema parseSchema(SchemaFormat sf){
		JsonSchema js = new JsonSchema();
		js.type = sf.type;
		if(sf.type == JsonValueType.ARRAY){
			js.items = parseSchema(sf.items);
		}
		else if(sf.type == JsonValueType.OBJECT){
			Map<String, Object> map = (Map<String, Object>)sf.properties;
			Iterator<Entry<String, Object> > it = map.entrySet().iterator();
			Entry<String, Object> ent;
			SchemaFormat tempsf;
			while(it.hasNext()){
				ent = it.next();
				//System.out.println(ent.getKey());
				tempsf = toSchemaFormat(ent.getValue());
				js.nameToSchema.put(ent.getKey(), parseSchema(tempsf));
//				if(type == JsonValueType.OBJECT) s.objectNameToSchema.put(ent.getKey(), parseSchema(sf.properties));
//				else if(type == JsonValueType.ARRAY){
//					SchemaFormat sf2 = (SchemaFormat)sf.items;
//					JsonValueType type2 = stringToJsonValueType.get(sf2.type);
//					s.arrayNameToType.put(ent.getKey(), type2);
//					if(type2 == JsonValueType.OBJECT) s.objectNameToSchema.put(ent.getKey(), parseSchema(sf2.properties));
//					else if(type2 == JsonValueType.ARRAY) throw new SemanticErrorException("array of array not supported yet");
//				}
			}
		}
		return js;
	}
	
	private static SchemaFormat toSchemaFormat(Object o){
		Map<String, Object> map = (Map<String, Object>)o;
		SchemaFormat sf = new SchemaFormat();
		Iterator<Entry<String, Object> > it = map.entrySet().iterator();
		Entry<String, Object> ent;
		while(it.hasNext()){
			ent = it.next();
			switch (ent.getKey()) {
			case "type":
				sf.type = Constants.stringToJsonValueType.get((String)ent.getValue());
				break;
				
			case "properties":
				sf.properties = ent.getValue();
				break;
				
			case "items":
				sf.items = toSchemaFormat(ent.getValue());
				break;

			default:
				break;
			}
		}
		
		return sf;
	}
	
	@Override
	public String toString(){
		return new Gson().toJson(this);
	}
	
	public boolean equals(JsonSchema schema){
		if((this.type == JsonValueType.INTEGER || this.type == JsonValueType.NUMBER) &&
			(schema.type == JsonValueType.INTEGER || schema.type == JsonValueType.NUMBER))
			return true;
		
		if(this.type != schema.type) return false;
		
		if(this.type == JsonValueType.ARRAY) return this.items.equals(schema.items);
		
		if(this.type == JsonValueType.OBJECT){
			if(this.nameToSchema.size() != schema.nameToSchema.size()) return false;
			Entry<String, JsonSchema> ent;
			Iterator<Entry<String, JsonSchema> > it = this.nameToSchema.entrySet().iterator();
			while(it.hasNext()){
				ent = it.next();
				if(! schema.nameToSchema.containsKey(ent.getKey())) return false;
				if(! ent.getValue().equals(schema.nameToSchema.get(ent.getKey()))) return false;
			}
			
			return true;
		}
		
		return true;
	}
	
	public boolean validate(Element ele){
		return validate(ele.jsonElement, this);
	}
	
	private boolean validate(JsonElement ele, JsonSchema schema){
		switch (schema.type) {
		case ARRAY:
			if(! ele.isJsonArray()) return false;
			else{
				Iterator<JsonElement> it = ele.getAsJsonArray().iterator();
				while(it.hasNext()){
					if(! validate(it.next(), schema.items)) return false;
				}
				return true;
			}
		case OBJECT:
			if(! ele.isJsonObject()) return false;
			else{
				JsonObject obj = ele.getAsJsonObject();
				Iterator<Entry<String, JsonSchema> > it = schema.nameToSchema.entrySet().iterator();
				Entry<String, JsonSchema> ent;
				while(it.hasNext()){
					ent = it.next();
					if(! obj.has(ent.getKey())) return false;
					if(! validate(obj.get(ent.getKey()), ent.getValue())) return false;
				}
				return true;
			}
		case NULL:
			if(! ele.isJsonNull()) return false;
			else return true;
		case BOOLEAN:
			if(! ele.isJsonPrimitive()) return false;
			else{
				Object o = Constants.gson.fromJson(ele, Object.class);
				if(o.getClass() == Boolean.class) return true;
				else return false;
			}
		case INTEGER:		//TODO how to tell?
		case NUMBER:
			if(! ele.isJsonPrimitive()) return false;
			else{
				Object o = Constants.gson.fromJson(ele, Object.class);
				if(o.getClass() == Double.class) return true;
				else return false;
			}
		case STRING:
			if(! ele.isJsonPrimitive()) return false;
			else{
				Object o = Constants.gson.fromJson(ele, Object.class);
				if(o.getClass() == String.class) return true;
				else return false;
			}
			
		default:
			return false;
		}
	}
	
	public JsonSchema getType(Object[] path){
		return getType(path, 0, this);
	}
	private JsonSchema getType(Object[] path, int index, JsonSchema schema){
		if(path.length == index) return schema;
		
		Object pathObj = path[index];
		if(pathObj.getClass() == String.class) return getType(path, index+1, schema.nameToSchema.get((String)pathObj));
		else if(pathObj.getClass() == Double.class && ((Double)pathObj).intValue() != -1) return getType(path, index+1, schema.items);
		else{
			JsonSchema retS = new JsonSchema();
			retS.type = JsonValueType.ARRAY;
			retS.items = getType(path, index+1, schema.items);
			return retS;
		}
	}
}
