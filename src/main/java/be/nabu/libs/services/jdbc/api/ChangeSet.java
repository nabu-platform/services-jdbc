package be.nabu.libs.services.jdbc.api;

import java.util.Map;

public interface ChangeSet {
	public Object getPrimaryKey();
	public ChangeType getType();
	public Map<String, Object> getOriginal();
	public Map<String, Object> getChanges();
}
