package cn.edu.zju.jsonStreamer.IO.input;

import cn.edu.zju.jsonStreamer.constants.SystemErrorException;

import com.google.gson.JsonElement;

public interface JStreamInput {
	//private JsonSchema schema;
	public String getNextElement() throws SystemErrorException;
	public boolean isEmpty();
	public void execute();
}
