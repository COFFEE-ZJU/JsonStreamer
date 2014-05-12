package cn.edu.zju.jsonStreamer.IO;

import com.google.gson.JsonElement;

import cn.edu.zju.jsonStreamer.constants.SystemErrorException;

public interface JStreamInput {
	//private JsonSchema schema;
	public JsonElement getNextElement() throws SystemErrorException;
	public boolean isEmpty();
	public void execute();
}
