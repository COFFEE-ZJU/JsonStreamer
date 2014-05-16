package cn.edu.zju.jsonStreamer.jsonAPI;
import java.io.Serializable;

import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.Constants.JsonCondType;

import com.google.gson.annotations.Expose;


public class JsonCondition implements Serializable{
	@Expose public String type = "CONDITION_OBJ";
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
