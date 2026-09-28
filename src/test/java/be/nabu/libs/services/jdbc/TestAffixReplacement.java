package be.nabu.libs.services.jdbc;

import java.util.Arrays;

import be.nabu.libs.services.jdbc.api.DataSourceWithAffixes.AffixMapping;
import junit.framework.TestCase;

public class TestAffixReplacement extends TestCase {
	public void testGenericAffixReplacement() {
		JDBCService service = new JDBCService("test");
		AffixMapping mapping = new AffixMapping();
		mapping.setAffix("tenant_");

		assertEquals("select * from tenant_customers", JDBCServiceInstance.replaceAffixes(service, Arrays.asList(mapping), "select * from ~customers"));
		assertEquals("select * from customerstenant_", JDBCServiceInstance.replaceAffixes(service, Arrays.asList(mapping), "select * from customers~"));
	}

	public void testPostgresqlTildeOperatorIsPreserved() {
		JDBCService service = new JDBCService("test");
		AffixMapping mapping = new AffixMapping();
		mapping.setAffix("tenant_");

		String sql = "select * from ~customers where name ~ :pattern";
		assertEquals("select * from tenant_customers where name ~ :pattern", JDBCServiceInstance.replaceAffixes(service, Arrays.asList(mapping), sql));
		assertEquals("select * from customers where name ~ :pattern", JDBCServiceInstance.replaceAffixes(service, null, sql));
	}
}
