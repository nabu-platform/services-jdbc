package be.nabu.libs.services.jdbc.api;

import java.util.List;

import javax.jws.WebParam;
import javax.validation.constraints.NotNull;

public interface ChangeTracker {
	public void track(@NotNull @WebParam(name = "connectionId") String connectionId, @WebParam(name = "transactionId") String transactionId, @NotNull @WebParam(name = "tableName") String tableName, @NotNull @WebParam(name = "changes") List<ChangeSet> changes);
}
