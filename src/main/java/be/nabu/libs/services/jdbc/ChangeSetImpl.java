package be.nabu.libs.services.jdbc;

import java.util.Map;

import be.nabu.libs.services.jdbc.api.ChangeSet;
import be.nabu.libs.services.jdbc.api.ChangeType;

public class ChangeSetImpl implements ChangeSet {

	private Object primaryKey;
	private ChangeType type;
	private Map<String, Object> changes, original;

	public ChangeSetImpl() {
		// auto
	}
	
	public ChangeSetImpl(Object primaryKey, ChangeType type, Map<String, Object> original, Map<String, Object> changes) {
		this.primaryKey = primaryKey;
		this.type = type;
		this.original = original;
		this.changes = changes;
	}
	
	@Override
	public Object getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(Object primaryKey) {
		this.primaryKey = primaryKey;
	}

	@Override
	public ChangeType getType() {
		return type;
	}
	public void setType(ChangeType type) {
		this.type = type;
	}

	@Override
	public Map<String, Object> getChanges() {
		return changes;
	}
	public void setChanges(Map<String, Object> changes) {
		this.changes = changes;
	}

	@Override
	public Map<String, Object> getOriginal() {
		return original;
	}
	public void setOriginal(Map<String, Object> original) {
		this.original = original;
	}
	
	
	
}
