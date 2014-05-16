package cn.edu.zju.jsonStreamer.parse;

import cn.edu.zju.jsonStreamer.IO.output.JStreamOutput;
import cn.edu.zju.jsonStreamer.constants.JsonStreamerException;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;

public abstract class Register {
	private boolean onlyOnce = false;
	public abstract void cancelQuery() throws SystemErrorException;
	public void register(String jsonApiString, JStreamOutput outStream) throws JsonStreamerException{
		if(onlyOnce) throw new SystemErrorException("register only allow once");
		onlyOnce = true;
	}
}
