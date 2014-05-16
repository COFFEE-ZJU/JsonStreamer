package cn.edu.zju.jsonStreamer.parse;

import cn.edu.zju.jsonStreamer.IO.output.JStreamOutput;
import cn.edu.zju.jsonStreamer.constants.JsonStreamerException;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;

public abstract class Register {
	public abstract void cancelQuery() throws SystemErrorException;
	public abstract void parse(String jsonApiString, JStreamOutput outStream) throws JsonStreamerException;
}
