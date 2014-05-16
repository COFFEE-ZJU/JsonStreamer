package cn.edu.zju.jsonStreamer.jsonAPI;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.Constants.JsonProjectionType;
import cn.edu.zju.jsonStreamer.json.JsonSchema;

import com.google.gson.annotations.Expose;


public class JsonProjection implements Serializable{
	@Expose public String type = "PROJECTION_OBJ";
//	public Boolean need_rename = null;
//	public String rename = null;
	@Expose public JsonProjectionType projection_type = null;
	@Expose public Map<String, JsonProjection> fields = null;		//for object type
	@Expose public List<JsonProjection> array_items = null;		//for array type
	@Expose public JsonExpression expression = null;		//for primitive type
	
	@Expose public JsonSchema retSchema = null;
	
	@Override
	public String toString(){
		return Constants.gson.toJson(this);
	}
}
