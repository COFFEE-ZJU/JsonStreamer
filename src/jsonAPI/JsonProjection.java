package jsonAPI;

import java.util.List;
import java.util.Map;

import json.JsonSchema;

//import others.JsonSchema;

import com.google.gson.annotations.Expose;

import constants.Constants.JsonProjectionType;

public class JsonProjection {
	@Expose public String type = "projection_obj";
//	public Boolean need_rename = null;
//	public String rename = null;
	@Expose public JsonProjectionType projection_type = null;
	@Expose public Map<String, JsonProjection> fields = null;		//for object type
	@Expose public List<JsonProjection> array_items = null;		//for array type
	@Expose public JsonExpression expression = null;		//for primitive type
	
	@Expose public JsonSchema retSchema = null;
}
