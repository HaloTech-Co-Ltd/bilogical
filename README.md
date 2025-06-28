## Bi-logical (bilogical) decoding plugin

Bi-logical (preview) is a logical decoding plugin that help to prevent replicating  
the same changes back and forth. The plugin is based on the work of pgoutput.  

## Supported Database

`Halo 1.0.14, 1.0.15`  
`openHalo 1.0`  
`PostgreSQL 14, 15`  

## Options

### bilogical.origin
Specifies whether the subscription will request the publisher to only send changes  
that don't have an origin or send changes regardless of origin. The value can be  
none, any. Setting to none means that the subscription will request the publisher  
to only send changes that don't have an origin. Setting to any means that the  
publisher sends changes regardless of their origin. The default is none.  

## How to use it ? (Example)

Suppose we have [bilogical1] and [bilogical2] two databases, and both database has  
a table named bitest.  
```
CREATE TABLE bitest(a INT, b TEXT);
```
The operations (INSERT/DELETE/UPDATE) on bitest in [bilogical1] will copy to  
[bilogical2] and vice versa. This will cause a cycled loop that version below  
PostgreSQL 16. With bilogical, the cycled loop can be prevented.  

```
------------        ------------
|bilogical1| <----> |bilogical2|
------------        ------------
```

### Step 1
Load the plugin when database startup. Add bilogical to the `shared_preload_libraries`.
```
...
shared_preload_libraries = 'bilogical'
...
```

### Step 2
Create `PUBLICATION` on each database.
```
-- bilogical1
CREATE PUBLICATION pubbi_a FOR TABLE bitest;
```

```
-- bilogical2
CREATE PUBLICATION pubbi_b FOR TABLE bitest;
```

### Step 3
Create `slot` on each database. The plugin must specified as bilogical!
```
-- bilogical1
SELECT pg_create_logical_replication_slot('lrs_subbib', 'bilogical');
```

```
-- bilogical2
SELECT pg_create_logical_replication_slot('lrs_subbia', 'bilogical');
```

### Step 4
Create `SUBSCRIPTION` on each database. Must use the slot that we created above!
```
-- bilogical1
CREATE SUBSCRIPTION subbib CONNECTION 'dbname=bilogical2 host=localhost port=1922 user=repuser'
PUBLICATION pubbi_b 
WITH (create_slot = false, slot_name = 'lrs_subbia');
```

```
-- bilogical2
CREATE SUBSCRIPTION subbia CONNECTION 'dbname=bilogical1 host=localhost port=1921 user=repuser'
PUBLICATION pubbi_a
WITH (create_slot = false, slot_name = 'lrs_subbib');
```
