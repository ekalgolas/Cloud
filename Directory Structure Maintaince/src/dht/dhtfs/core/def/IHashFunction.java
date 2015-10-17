package dht.dhtfs.core.def;

import java.io.Serializable;

public interface IHashFunction extends Serializable {

	public int hashValue(String name);

	public String getDescription(String description);

	public void setDescription(String description);

}
