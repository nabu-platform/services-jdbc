# Support for arrays

It is generally a very hard problem to do something like "where a in (?)" in JDBC. There are a number of bastardized solutions:

- execute single queries and combine at application level (or using UNION)
- execute stored procedure
- create prepared statement dynamically (so vary the number of "?" at runtime). The major downside of this is that you bypass a lot of the query caching etc that JDBC has so this can have a performance impact
- an optimized solution of the last where you generate a fixed amount of "?" and fill in NULL if not available

Alternatively some databases allow you to simply do this:

```
preparedStatement.setObject(1, new Object[] { "a", "b" }, Types.VARCHAR);
```

Postgresql however is not one of them.

I tried variations like typing the array:

```
preparedStatement.setObject(1, new String[] { "a", "b" }, Types.VARCHAR);
```

Or changing the sql type:

```
preparedStatement.setObject(1, new Object[] { "a", "b" }, Types.ARRAY);
```

But nothing works in postgresql using the latest driver (@2015-11-24: postgresql-9.4-1205.jdbc42.jar)

## ANY vs IN

However there is something that does work, at least in some databases (including postgresql):

```
java.sql.Array array = connection.createArrayOf("varchar", collection.toArray());
statement.setObject(index++, array, Types.ARRAY);
```

We create an array using the driver capabilities (normally reserved for database-level arrays).
Then we set the array with the array type.

The name of the type can be database specific but generic types should (hopefully) be compatible across databases (otherwise it has to move to the dialect). 

The key point here however is that the `IN` operator **still does not work**. So this will fail: `org.postgresql.util.PSQLException: ERROR: operator does not exist: character varying = character varying[]`

What **does** work however and is pretty standardized among big databases is the ANY method (there is also a SOME and ALL):

```
select first_name from user where last_name = any(:lastNames)
```

Some references online can be found that you need to explicitly cast to `varchar[]` for it to work:

```
where a = ANY(?::varchar[])
```

But seems superfluous for our usecase.