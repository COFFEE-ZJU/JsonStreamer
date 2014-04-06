package jsonAPI;

import java.util.List;

import json.JsonSchema;

//import others.JsonSchema;

import com.google.gson.annotations.Expose;

import constants.Constants.JsonProjectionType;

public class JsonProjection {
	@Expose public String type = "projection_obj";
	@Expose public Boolean need_rename = null;
	@Expose public String rename = null;
	@Expose public JsonProjectionType projection_type = null;
	@Expose public List<JsonProjection> fields = null;		//for object type
	@Expose public List<JsonProjection> array_items = null;		//for array type
	@Expose public JsonExpression expression = null;		//for primitive type
	
	@Expose public JsonSchema retSchema = null;
}
