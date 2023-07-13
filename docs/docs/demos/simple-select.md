# Simple Select

The simplest demo included in our set is one that simply takes a 

 that still serves to
demonstrate the basics of DBSP.  We will 

Consider the following SQL, which defines a table named `users` and a
view named `output_users`:

```sql
CREATE TABLE users (name varchar);

CREATE VIEW output_users AS SELECT * FROM users;
```
