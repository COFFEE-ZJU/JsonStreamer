package cn.edu.zju.jsonStreamer.jsonAPI;

import java.util.List;

import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.Constants.AggrFuncNames;
import cn.edu.zju.jsonStreamer.constants.Constants.JsonAttrSource;
import cn.edu.zju.jsonStreamer.constants.Constants.JsonExprType;
import cn.edu.zju.jsonStreamer.constants.SemanticErrorException;
import cn.edu.zju.jsonStreamer.json.JsonSchema;

import com.google.gson.annotations.Expose;



public class JsonExpression {
	@Expose public String type = "EXPRESSION_OBJ";
	@Expose public JsonExprType expression_type = null;		//ID, INT, NUMBER, BOOL, NULL, STRING, ADD, SUB, DIV, MUL, MOD, AGGREGATION
	@Expose public AggrFuncNames aggregate_operation = null;
	@Expose public JsonProjection aggregate_projection = null;
	@Expose public JsonExpression left = null;
	@Expose public JsonExpression right = null;
	@Expose public List<Object> id_name = null;
	@Expose public String string_value = null;
	@Expose public Integer int_value = null;
	@Expose public Double number_value = null;
	@Expose public Boolean bool_value = null;
	@Expose public JsonAttrSource attribute_source = null;		//for join operation only
	
	@Expose public JsonSchema retSchema = null;
//	public boolean lastNameIsArray = true;		//for id type only
//	
	public String getLastIdName() throws SemanticErrorException{
		if(expression_type != JsonExprType.ID) throw new SemanticErrorException("expression is not id type");
		if(id_name.size()==0) return "";
		if(id_name.get(id_name.size()-1).getClass() == String.class) return (String)id_name.get(id_name.size()-1);
		else throw new SemanticErrorException("need a field name");
	}
	
	@Override
	public String toString(){
		return Constants.gson.toJson(this);
	}
//	Constants.JsonValueType retType = null;
//	Constants.JsonValueType arrayDataType = null;
//	JsonSchema objectSchema = null;
}
