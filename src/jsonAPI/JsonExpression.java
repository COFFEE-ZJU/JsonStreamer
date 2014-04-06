package jsonAPI;

import java.util.List;

import json.JsonSchema;

//import others.JsonSchema;
import com.google.gson.annotations.Expose;

//import constants.SemanticErrorException;
import constants.Constants;
import constants.Constants.AggrFuncNames;
import constants.Constants.CondType;
import constants.Constants.ExprType;
import constants.Constants.JsonAttrSource;


public class JsonExpression {
	@Expose public String type = "expression_obj";
	@Expose public String expression_type = null;		//id, int, number, bool, null, string, add, sub, div, mul, mod, aggregation;
	public ExprType exprType = null;
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
	
	public ExprType getType(){
		if(exprType == null) exprType = Constants.gson.fromJson(Constants.gson.toJson(expression_type).toUpperCase(), ExprType.class);
		
		return exprType;
	}
	
	@Expose public JsonSchema retSchema = null;
//	public boolean lastNameIsArray = true;		//for id type only
//	
//	public String getLastIdName(){
//		if(! expression_type.equals("id")) throw new SemanticErrorException("expression is not id type");
//		if(lastNameIsArray) throw new SemanticErrorException("need a field name");
//		if(id_name.size()==0) return "";
//		else return (String)id_name.get(id_name.size()-1);
//	}
	
//	Constants.JsonValueType retType = null;
//	Constants.JsonValueType arrayDataType = null;
//	JsonSchema objectSchema = null;
}
