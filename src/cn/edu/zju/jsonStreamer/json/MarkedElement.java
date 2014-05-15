package cn.edu.zju.jsonStreamer.json;

import cn.edu.zju.jsonStreamer.constants.Constants.ElementMark;

public class MarkedElement {
	public final Element element;
	public final long id;
	public ElementMark mark;
	public final long timeStamp;
	public MarkedElement(Element ele, long id, ElementMark mark, long timeStamp){
		element = ele;
		this.id = id;
		this.mark = mark;
		this.timeStamp = timeStamp;
	}
}