package jsonAPI;
import com.google.gson.annotations.Expose;

import constants.Constants;
import constants.Constants.JsonCondType;

public class JsonCondition {
	@Expose public String type = "condition_obj";
	@Expose public JsonCondType condition_type = null;		//AND, OR, NOT, GT, GE, LT, LE, EQ, NE, BOOL
	@Expose public JsonCondition condition = null;
	@Expose public JsonCondition left_condition = null;
	@Expose public JsonCondition right_condition = null;
	@Expose public JsonExpression left_expression = null;
	@Expose public JsonExpression right_expression = null;
	@Expose public JsonExpression bool_expression = null;
	
	@Override
	public String toString(){
		return Constants.gson.toJson(this);
	}
}
