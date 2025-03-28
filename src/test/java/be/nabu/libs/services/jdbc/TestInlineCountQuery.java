package be.nabu.libs.services.jdbc;

import junit.framework.TestCase;

public class TestInlineCountQuery extends TestCase {
	
	public void testSimple() {
		String sql = "select name from tests";
		String expected = "select name,\n"
				+ "	count(1) over () as injected_inline_total_count\n"
				+ "from tests";
		String injectTotalCount = JDBCService.injectTotalCount(sql);
		assertEquals(expected, injectTotalCount);
	}
	
	public void testOrganisationList() {
		String sql = "select distinct \n"
				+ "	n.id,\n"
				+ "	n.created,\n"
				+ "	n.modified,\n"
				+ "	n.started,\n"
				+ "	n.stopped,\n"
				+ "	n.verified,\n"
				+ "	n.parent_id,\n"
				+ "	n.owner_id,\n"
				+ "	n.enabled,\n"
				+ "	max(n.version),\n"
				+ "	n.priority,\n"
				+ "	n.name,\n"
				+ "	n.title,\n"
				+ "	n.description,\n"
				+ "	n.path,\n"
				+ "	n.slug,\n"
				+ "	n.language_id,\n"
				+ "	n.component_id,\n"
				+ "	n.anonymized,\n"
				+ "	o.organisation_type_id,\n"
				+ "	o.commercial_name,\n"
				+ "	o.established,\n"
				+ "	o.legal_form_id,\n"
				+ "	o.legal_form_other,\n"
				+ "	o.legal_system_id,\n"
				+ "	o.legal_system_other,\n"
				+ "	o.branch_office,\n"
				+ "	o.email,\n"
				+ "	o.phone_number,\n"
				+ "	o.website,\n"
				+ "	o.webshop,\n"
				+ "	o.calendar_id,\n"
				+ "	o.education_type_id,\n"
				+ "	o.part_of_group,\n"
				+ "	o.group_name,\n"
				+ "	o.vat_required,\n"
				+ "	o.invoice_address_same,\n"
				+ "	o.invoice_email,\n"
				+ "	o.invoice_additional_emails,\n"
				+ "	o.invoice_name,\n"
				+ "	o.variant,\n"
				+ "	o.country_id,\n"
				+ "	o.digital_communication_only,\n"
				+ "	o.digital_invoices_only,\n"
				+ "	o.application_ids,\n"
				+ "	o.declaration_frequency_id,\n"
				+ "	o.battery_weight20_id,\n"
				+ "	o.battery_weight200_id,\n"
				+ "	o.yearly_battery_amount_id,\n"
				+ "	o.sold_batteries_in_past3_years,\n"
				+ "	o.battery_origin_ids,\n"
				+ "	o.battery_destination_ids,\n"
				+ "	o.vat_business_category_id,\n"
				+ "	o.individual_contract,\n"
				+ "	o.charter_state,\n"
				+ "	o.participant_agreement_state_id,\n"
				+ "	o.participant_since,\n"
				+ "	o.participant_state,\n"
				+ "	o.collector_state,\n"
				+ "	o.declarations_synced,\n"
				+ "	o.points,\n"
				+ "	o.points_to_expire,\n"
				+ "	o.points_expiration\n"
				+ "\n"
				+ "from nodes n\n"
				+ "join organisations o on n.id = o.id\n"
				+ "left join node_external_ids nei on nei.node_id = o.id and nei.external_id_type_id = any(?::uuid[])\n"
				+ "join (\n"
				+ "    select replace('%' || n.id || '%', '-', '') as match from nodes n join node_connections nc on n.id = nc.target_id join group_roles gr on gr.node_id = nc.source_id\n"
				+ "        join action_roles ar on ar.role_id = gr.role_id\n"
				+ "        join actions a on a.id = ar.action_id\n"
				+ "        join user_groups ug on ug.group_id = gr.group_id\n"
				+ "    where a.name = 'organisation.list'\n"
				+ "        and ug.user_id = ?::uuid\n"
				+ "    union all\n"
				+ "    select replace('%' || n.id || '%', '-', '') as match from nodes n join group_roles gr on gr.node_id = n.id\n"
				+ "        join action_roles ar on ar.role_id = gr.role_id\n"
				+ "        join actions a on a.id = ar.action_id\n"
				+ "        join user_groups ug on ug.group_id = gr.group_id\n"
				+ "    where a.name = 'organisation.list'\n"
				+ "        and ug.user_id = ?::uuid\n"
				+ "    union all\n"
				+ "    select '%' from group_roles gr\n"
				+ "        join action_roles ar on ar.role_id = gr.role_id\n"
				+ "        join actions a on a.id = ar.action_id\n"
				+ "        join user_groups ug on ug.group_id = gr.group_id\n"
				+ "    where a.name = 'organisation.list'\n"
				+ "        and ug.user_id = ?::uuid\n"
				+ "        and gr.node_id is null\n"
				+ ") authorized_nodes on n.path like authorized_nodes.match\n"
				+ "where\n"
				+ "	n.enabled = true\n"
				+ "	and (\n"
				+ "		? is null\n"
				+ "		or lower(nei.external_id) like '%' || lower(?) || '%'\n"
				+ "		or lower(n.name) like '%' || lower(?) || '%'\n"
				+ "		or lower(o.commercial_name) like '%' || lower(?) || '%'\n"
				+ "	)\n"
				+ "	and (?::uuid[] is null or n.id = any(?::uuid[]))\n"
				+ " ORDER BY 12 asc OFFSET 0 LIMIT 10";
		String expected = "select *, count(1) over () as injected_inline_total_count from (select distinct \n"
				+ "	n.id,\n"
				+ "	n.created,\n"
				+ "	n.modified,\n"
				+ "	n.started,\n"
				+ "	n.stopped,\n"
				+ "	n.verified,\n"
				+ "	n.parent_id,\n"
				+ "	n.owner_id,\n"
				+ "	n.enabled,\n"
				+ "	max(n.version),\n"
				+ "	n.priority,\n"
				+ "	n.name,\n"
				+ "	n.title,\n"
				+ "	n.description,\n"
				+ "	n.path,\n"
				+ "	n.slug,\n"
				+ "	n.language_id,\n"
				+ "	n.component_id,\n"
				+ "	n.anonymized,\n"
				+ "	o.organisation_type_id,\n"
				+ "	o.commercial_name,\n"
				+ "	o.established,\n"
				+ "	o.legal_form_id,\n"
				+ "	o.legal_form_other,\n"
				+ "	o.legal_system_id,\n"
				+ "	o.legal_system_other,\n"
				+ "	o.branch_office,\n"
				+ "	o.email,\n"
				+ "	o.phone_number,\n"
				+ "	o.website,\n"
				+ "	o.webshop,\n"
				+ "	o.calendar_id,\n"
				+ "	o.education_type_id,\n"
				+ "	o.part_of_group,\n"
				+ "	o.group_name,\n"
				+ "	o.vat_required,\n"
				+ "	o.invoice_address_same,\n"
				+ "	o.invoice_email,\n"
				+ "	o.invoice_additional_emails,\n"
				+ "	o.invoice_name,\n"
				+ "	o.variant,\n"
				+ "	o.country_id,\n"
				+ "	o.digital_communication_only,\n"
				+ "	o.digital_invoices_only,\n"
				+ "	o.application_ids,\n"
				+ "	o.declaration_frequency_id,\n"
				+ "	o.battery_weight20_id,\n"
				+ "	o.battery_weight200_id,\n"
				+ "	o.yearly_battery_amount_id,\n"
				+ "	o.sold_batteries_in_past3_years,\n"
				+ "	o.battery_origin_ids,\n"
				+ "	o.battery_destination_ids,\n"
				+ "	o.vat_business_category_id,\n"
				+ "	o.individual_contract,\n"
				+ "	o.charter_state,\n"
				+ "	o.participant_agreement_state_id,\n"
				+ "	o.participant_since,\n"
				+ "	o.participant_state,\n"
				+ "	o.collector_state,\n"
				+ "	o.declarations_synced,\n"
				+ "	o.points,\n"
				+ "	o.points_to_expire,\n"
				+ "	o.points_expiration\n"
				+ "\n"
				+ "from nodes n\n"
				+ "join organisations o on n.id = o.id\n"
				+ "left join node_external_ids nei on nei.node_id = o.id and nei.external_id_type_id = any(?::uuid[])\n"
				+ "join (\n"
				+ "    select replace('%' || n.id || '%', '-', '') as match from nodes n join node_connections nc on n.id = nc.target_id join group_roles gr on gr.node_id = nc.source_id\n"
				+ "        join action_roles ar on ar.role_id = gr.role_id\n"
				+ "        join actions a on a.id = ar.action_id\n"
				+ "        join user_groups ug on ug.group_id = gr.group_id\n"
				+ "    where a.name = 'organisation.list'\n"
				+ "        and ug.user_id = ?::uuid\n"
				+ "    union all\n"
				+ "    select replace('%' || n.id || '%', '-', '') as match from nodes n join group_roles gr on gr.node_id = n.id\n"
				+ "        join action_roles ar on ar.role_id = gr.role_id\n"
				+ "        join actions a on a.id = ar.action_id\n"
				+ "        join user_groups ug on ug.group_id = gr.group_id\n"
				+ "    where a.name = 'organisation.list'\n"
				+ "        and ug.user_id = ?::uuid\n"
				+ "    union all\n"
				+ "    select '%' from group_roles gr\n"
				+ "        join action_roles ar on ar.role_id = gr.role_id\n"
				+ "        join actions a on a.id = ar.action_id\n"
				+ "        join user_groups ug on ug.group_id = gr.group_id\n"
				+ "    where a.name = 'organisation.list'\n"
				+ "        and ug.user_id = ?::uuid\n"
				+ "        and gr.node_id is null\n"
				+ ") authorized_nodes on n.path like authorized_nodes.match\n"
				+ "where\n"
				+ "	n.enabled = true\n"
				+ "	and (\n"
				+ "		? is null\n"
				+ "		or lower(nei.external_id) like '%' || lower(?) || '%'\n"
				+ "		or lower(n.name) like '%' || lower(?) || '%'\n"
				+ "		or lower(o.commercial_name) like '%' || lower(?) || '%'\n"
				+ "	)\n"
				+ "	and (?::uuid[] is null or n.id = any(?::uuid[]))\n"
				+ " ORDER BY 12 asc OFFSET 0 LIMIT 10) a";
		String injectTotalCount = JDBCService.injectTotalCount(sql);
		assertEquals(expected, injectTotalCount);
	}
}
