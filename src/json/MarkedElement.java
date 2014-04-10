package json;

import constants.Constants.ElementMark;

public class MarkedElement {
	public final Element element;
	public final long id;
	public final ElementMark mark;
	public MarkedElement(Element ele, long id, ElementMark mark){
		element = ele;
		this.id = id;
		this.mark = mark;
	}
}
