package jsonAPI;

import com.google.gson.annotations.Expose;

import constants.Constants.ErrorType;

public class JsonError {
	@Expose public final String type = "ERROR_OBJ";
	@Expose public final ErrorType error_type;
	@Expose public final String error_message;
	
	public JsonError(ErrorType type, String message){
		error_type = type;
		error_message = message; 
	}
}
