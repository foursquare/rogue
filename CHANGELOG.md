## Release Notes

# 1.0.14:

- index hinting support
- support for selecting subfields (MongoMapField and MongoCaseClassField only; no support for MongoCaseClassListField)
- "between" convenience operator (numeric)
- scala 2.9.0 and 2.9.0-1 build support -- thanks eltimn!

# 1.0.13:

- fixed ObjectId construction for date ranges by zeroing out machine, pid and counter fields
- support for $maxScan and $comment addSpecial parameters on find() queries

# 1.0.12:

- always specify field names to return in the query; if select() was not specified, use all field names from the model
- some code cleanup (use case classes and copy() to save some typing)

# 1.0.11:

- explain() method on BaseQuery (thanks tjulien!)
- support for select()ing up to 6 fields

# 1.0.10:

- regression fix for 1.0.9

# 1.0.9

- added hooks for full query validation
- support for $type and $mod query operators
- query signatures: string version of a query without values
- support for indicating when a query clause is intended to hit an index (for runtime index checking, if you wish to implement it)

# 1.0.8

- extra logging around mongo exceptions

# 1.0.7

- support for empty (noop) modify queries

# 1.0.6

- fetchBatch now uses db cursors
- building against Lift snapshot (thanks Indrajit!)
- support for crossbuilding 2.8.0 & 2.8.1

# 1.0.5

- added tiny bit more type safety to unsafeField subfield selector

# 1.0.4

- bug fix: alwasy set _id in select() queries

# 1.0.3

- fixed setTo serialization
- eqs/neqs support for GeoQueryField

# 1.0.2

- support for querying sub-fields of a map

# 1.0.1

- added BasePaginatedQuery.setCountPerPage()

