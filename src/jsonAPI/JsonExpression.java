package jsonAPI;

import java.util.List;

import json.JsonSchema;

//import others.JsonSchema;
import com.google.gson.annotations.Expose;

import constants.Constants.AggrFuncNames;
import constants.Constants.JsonAttrSource;
import constants.Constants.JsonExprType;
import constants.SemanticErrorException;


public class JsonExpression {
	@Expose public String type = "expression_obj";
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
	public String getLastIdName(){
		if(expression_type != JsonExprType.ID) throw new SemanticErrorException("expression is not id type");
		if(id_name.size()==0) return "";
		if(id_name.get(id_name.size()-1).getClass() == String.class) return (String)id_name.get(id_name.size()-1);
		else throw new SemanticErrorException("need a field name");
	}
	
//	Constants.JsonValueType retType = null;
//	Constants.JsonValueType arrayDataType = null;
//	JsonSchema objectSchema = null;
}
