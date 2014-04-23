package IO;

import constants.SystemErrorException;
import json.Element;

public interface JStreamInput {
	//private JsonSchema schema;
	public Element getNextElement() throws SystemErrorException;
	public boolean isEmpty();
	public void execute();
}
