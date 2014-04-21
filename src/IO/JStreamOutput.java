package IO;

import json.MarkedElement;

public interface JStreamOutput {
	public boolean pushNext(MarkedElement ele);
	public boolean isFull();
	public void execute();
}
