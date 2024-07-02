package be.nabu.libs.services.jdbc;

import be.nabu.libs.services.jdbc.api.JDBCTranslator.TranslationBinding;
import be.nabu.libs.types.SimpleTypeWrapperFactory;
import be.nabu.libs.types.api.DefinedSimpleType;
import be.nabu.libs.types.base.SimpleElementImpl;
import be.nabu.libs.types.base.ValueImpl;
import be.nabu.libs.types.properties.PrimaryKeyProperty;
import be.nabu.libs.types.properties.TranslatableProperty;
import be.nabu.libs.types.structure.Structure;
import junit.framework.TestCase;

public class TestTranslationBinding extends TestCase {
	public void testBinding() {
		Structure structure = new Structure();
		DefinedSimpleType<String> wrap = SimpleTypeWrapperFactory.getInstance().getWrapper().wrap(String.class);
		structure.add(new SimpleElementImpl<String>("id", wrap, structure, new ValueImpl<Boolean>(PrimaryKeyProperty.getInstance(), true)));
		structure.add(new SimpleElementImpl<String>("name", wrap, structure));
		structure.add(new SimpleElementImpl<String>("title", wrap, structure, new ValueImpl<Boolean>(TranslatableProperty.getInstance(), true)));
		
		TranslationBinding binding = new TranslationBinding();
		binding.setTranslationTable("translations");
		binding.setFieldName("name");
		binding.setFieldId("instance_id");
		binding.setFieldTranslation("translation");
		binding.setFieldLanguage("language_id");
		
//		String rewriteTranslated = JDBCServiceInstance.rewriteTranslated("select t.id, t.name, t.title from tests t where t.title like '%test%'", structure, binding, "en");
//		System.out.println(rewriteTranslated);
		
		JDBCServiceInstance.rewriteTranslated("select \n" + 
				"	mde.id,\n" + 
				"	mde.created,\n" + 
				"	mde.modified,\n" + 
				"	mde.name,\n" + 
				"	mde.title,\n" + 
				"	mde.description,\n" + 
				"	mde.owner_id,\n" + 
				"	mde.master_data_category_id,\n" + 
				"	mde.priority,\n" + 
				"	mde.operational,\n" + 
				"	mde.disabled,\n" + 
				"	f0_master_data_category_id.name as master_data_category\n" + 
				" from  master_data_entries mde\n" + 
				"	 join master_data_categories f0_master_data_category_id on f0_master_data_category_id.id = mde.master_data_category_id  LIMIT 100", structure, binding, "en");
	}
	
	public void testBinding2() {
		Structure structure = new Structure();
		DefinedSimpleType<String> wrap = SimpleTypeWrapperFactory.getInstance().getWrapper().wrap(String.class);
		structure.add(new SimpleElementImpl<String>("id", wrap, structure, new ValueImpl<Boolean>(PrimaryKeyProperty.getInstance(), true)));
		structure.add(new SimpleElementImpl<String>("favorite", wrap, structure));
		structure.add(new SimpleElementImpl<String>("title1", wrap, structure, new ValueImpl<Boolean>(TranslatableProperty.getInstance(), true)));
		structure.add(new SimpleElementImpl<String>("title2", wrap, structure, new ValueImpl<Boolean>(TranslatableProperty.getInstance(), true)));
		structure.add(new SimpleElementImpl<String>("title3", wrap, structure, new ValueImpl<Boolean>(TranslatableProperty.getInstance(), true)));
		structure.add(new SimpleElementImpl<String>("title4", wrap, structure, new ValueImpl<Boolean>(TranslatableProperty.getInstance(), true)));
		structure.add(new SimpleElementImpl<String>("title5", wrap, structure, new ValueImpl<Boolean>(TranslatableProperty.getInstance(), true)));
		
		TranslationBinding binding = new TranslationBinding();
		binding.setTranslationTable("translations");
		binding.setFieldName("name");
		binding.setFieldId("instance_id");
		binding.setFieldTranslation("translation");
		binding.setFieldLanguage("language_id");
		
		String rewriteTranslated = JDBCServiceInstance.rewriteTranslated("select\n" + 
				"	bt.id,\n" + 
				"	btf.id is not null as favorite,\n" + 
				"	mde2.title,\n" + 
				"	mde3.title,\n" + 
				"	mde4.title,\n" + 
				"	mde5.title,\n" + 
				"	mde6.title\n" + 
				"from battery_types bt\n" + 
				"	-- We assume that there will never be overlapping time intervals for price\n" + 
				"	join battery_type_prices btp on btp.battery_type_id = bt.id and btp.started <= ?::timestamp and ?::timestamp < btp.stopped\n" + 
				"	join master_data_entries mde2 on mde2.id = bt.chemical_family_id\n" + 
				"	join master_data_entries mde3 on mde3.id = bt.battery_category_id\n" + 
				"	join master_data_entries mde5 on mde5.id = bt.battery_sold_as_id\n" + 
				"	left outer join master_data_entries mde4 on mde4.id = bt.battery_parent_category_id\n" + 
				"	left outer join master_data_entries mde6 on mde6.id = bt.battery_usage_id\n" + 
				"	left outer join battery_type_favorites btf on btf.battery_type_id = bt.id\n" + 
				"		and btf.organisation_id = ?::uuid\n" + 
				"		and btf.user_id = ?::uuid\n" + 
				"where\n" + 
				"	(?::uuid[] is null or bt.id = any(?::uuid[]))\n" + 
				"	and (?::text[] is null or bt.name = any(?::text[]))\n" + 
				"	and (? is null or lower(bt.name) like ('%' || lower(?) || '%'))\n" + 
				"	and (?::text[] is null or bt.iec_code = any(?::text[]))\n" + 
				"	and (?::boolean is null or bt.rechargeable = ?::boolean)\n" + 
				"	and (?::uuid[] is null or bt.chemical_family_id = any(?::uuid[]))\n" + 
				"	and (?::uuid[] is null or bt.battery_category_id = any(?::uuid[]))\n" + 
				"	and (?::uuid[] is null or bt.battery_usage_id = any(?::uuid[]))\n" + 
				"	and (?::uuid[] is null or bt.battery_sold_as_id = any(?::uuid[]))\n" + 
				"	and (?::uuid is null or ?::uuid = any(bt.declaration_frequency_id))\n" + 
				"	and (? is null or unit_weight >= ? or min_weight >= ? or max_weight >= ?)\n" + 
				"	and (? is null or unit_weight <= ? or min_weight <= ? or max_weight <= ?)\n" + 
				"	and (?::boolean is null or (?::boolean = true and btf.id is not null))\n" + 
				"	and (?::boolean is null or bt.requires_serial_number = ?::boolean)\n" + 
				"	and (?::uuid[] is null or bt.battery_format_id = any(?::uuid[]))\n" + 
				"	and (?::uuid[] is null or bt.battery_parent_category_id = any(?::uuid[])) ORDER BY 2 asc OFFSET 0 LIMIT 20\n" + 
				"", structure, binding, "en");
		
		System.out.println(rewriteTranslated);
	}
}
