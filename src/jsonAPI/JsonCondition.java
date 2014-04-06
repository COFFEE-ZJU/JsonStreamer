package jsonAPI;
import com.google.gson.annotations.Expose;

import constants.Constants.CondType;
import constants.Constants;

public class JsonCondition {
	@Expose public String type = "condition_obj";
	@Expose public String condition_type = null;		//and, or, not, gt, ge, lt, le, eq, ne, bool
	public CondType condType = null;
	@Expose public JsonCondition condition = null;
	@Expose public JsonCondition left_condition = null;
	@Expose public JsonCondition right_condition = null;
	@Expose public JsonExpression left_expression = null;
	@Expose public JsonExpression right_expression = null;
	@Expose public JsonExpression bool_expression = null;
	
	public CondType getType(){
		if(condType == null) condType = Constants.gson.fromJson(Constants.gson.toJson(condition_type).toUpperCase(), CondType.class);
		
		return condType;
	}
}
