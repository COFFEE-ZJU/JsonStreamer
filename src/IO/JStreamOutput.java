package IO;

import constants.SystemErrorException;
import json.MarkedElement;

public interface JStreamOutput {
	public boolean pushNext(MarkedElement ele) throws SystemErrorException;
	public boolean isFull();
	public void execute();
}
