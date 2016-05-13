package be.nabu.libs.services.jdbc;

public class UpdateResult {
	private int affectedRows;
	
	public UpdateResult(int affectedRows) {
		this.affectedRows = affectedRows;
	}

	public int getAffectedRows() {
		return affectedRows;
	}
}
