package cn.edu.zju.jsonStreamer.IO;

import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.Element;

public interface JStreamInput {
	//private JsonSchema schema;
	public Element getNextElement() throws SystemErrorException;
	public boolean isEmpty();
	public void execute();
}
