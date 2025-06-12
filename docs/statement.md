# Statement-Level Triggers

Statement-level triggers provide the ability to run triggers once-per statement, offering a significant performance advantage over row-based triggers. There are some notable differences:

1. Statement-level triggers cannot be conditionally executed, making it more cumbersome to express triggers based on changes.
2. Statement-level triggers can only fire after an operation. They cannot alter rows in memory or cause Postgres to ignore certain operations.
3. There is no guaranteed ordering of the old and new rows in [transition tables](https://dba.stackexchange.com/questions/177463/what-is-a-transition-table-in-postgres). In order to detect differences between old and new, we must join based on primary key. If the primary key is updated, we miss out on these changes.

Although one can verbosely express statement-level triggers with [pgtrigger.Trigger][] classes (see [the cookbook for an example](cookbook.md#statement-level-triggers-and-transition-tables)), here we focus on [pgtrigger.Composer][], a utility class that allows one to architect row and statement-level triggers in a similar way.

We cover how this class automatically handles `REFERENCING` declarations and provides utilities for expressing conditions for statement-level triggers, bringing them more close to their row-level counterparts without the performance implications.

## Automatic references declaration

[pgtrigger.Composer][] limits the boilerplate of statement-level triggers by automatically creating a `references` when `level=pgtrigger.Statement` is used. The `references` declaration makes `old_values` and `new_values` tables available based on the combinations of the operations provided.

For example:

- `pgtrigger.Update` makes `references=pgtrigger.References(old='old_values', new='new_values')`
- `pgtrigger.Delete` makes `references=pgtrigger.References(old='old_values')`
- `pgtrigger.Insert` makes `pgtrigger.References(new='new_values')`

`pgtrigger.UpdateOf` and `pgtrigger.Truncate` always generate null references.


!!! note

    Combinations of operations is not supported by Postgres when using transition tables, so operations like `pgtrigger.Update | pgtrigger.Delete` will result in no transition tables being declared.

For example, here we write a history tracking trigger that bulk inserts old and new fields from the `old_values` and `new_values` transition tables:

```python
pgtrigger.Composer(
    name="track_history",
    level=pgtrigger.Statement,
    when=pgtrigger.After,
    operation=pgtrigger.Update,
    func=f"""
        INSERT INTO {HistoryModel._meta.db_table}(old_field, new_field)
        SELECT
            old_values.field AS old_field,
            new_values.field AS new_field
        FROM old_values
            JOIN new_values ON old_values.id = new_values.id;
        RETURN NULL;
    """
)
```

## Template variables when using conditions

Conditions are not possible on statement-level trigger definitions in Postgres like row-level counterparts. Standard statement-level trigger definitions with [pgtrigger.Trigger][] and a `condition` supplied will fail.

[pgtrigger.Composer][], however, handles the `condition` argument in a special manner for statement-level triggers, providing the following template variables in [pgtrigger.Func][] that help one conditionally access the transition tables:

- **cond_old_values**: A fragment that has the `old_values` alias filtered by the condition.
- **cond_new_values**: A fragment that has the `new_values` alias filtered by the condition.
- **cond_joined_values**: A fragment that always has both `old_values` and `new_values` aliases joined and filtered by the condition.

Use the minimum alias needed in your trigger to ensure best performance of generated SQL. If your trigger, for example, only needs to access conditionally-filtered old rows, use `cond_old_values` to ensure most optimal SQL.

Here's an example of a conditonal statement-level update trigger:

```python
pgtrigger.Composer(
    name="composer_protect",
    level=pgtrigger.Statement,
    when=pgtrigger.After,
    operation=pgtrigger.Update,
    declare=[("val", "RECORD")],
    func=pgtrigger.Func(
        """
        FOR val IN SELECT new_values.* FROM {cond_new_values}
        LOOP
            RAISE EXCEPTION 'uh oh';
        END LOOP;
        RETURN NULL;
        """
    ),
    condition=pgtrigger.Q(new__int_field__gt=0, old__int_field__lt=100),
)
```

In the above, the expanded PL/pgSQL looks like this:

```sql
FOR val IN
    SELECT new_values.* FROM old_values
    JOIN new_values ON old_values.id = new_values.id
    WHERE new_values.int_field > 0 AND old_values.int_field < 100
LOOP
    RAISE EXCEPTION 'uh oh';
END LOOP;
RETURN NULL;
```

Since the condition spans old and new, `{cond_new_values}` automatically joins these reference tables. If we simplify our condition to not require old values, `{cond_new_values}` becomes simpler too:

```python
pgtrigger.Composer(
    name="composer_protect",
    level=pgtrigger.Statement,
    when=pgtrigger.After,
    operation=pgtrigger.Update,
    declare=[("val", "RECORD")],
    func=pgtrigger.Func(
        """
        FOR val IN SELECT new_values.* FROM {cond_new_values}
        LOOP
            RAISE EXCEPTION 'uh oh';
        END LOOP;
        RETURN NULL;
        """
    ),
    condition=pgtrigger.Q(new__int_field__gt=0),
)
```

In the above, the expanded PL/pgSQL looks like this:

```sql
FOR val IN
    SELECT new_values.* FROM new_values
    WHERE new_values.int_field > 0
LOOP
    RAISE EXCEPTION 'uh oh';
END LOOP;
RETURN NULL;
```

Remember the following key points when using these variables:

1. `django-pgtrigger` renders three template variables for different use cases. Depending on the condition, fragments may be simpler if they don't span old and new rows.
2. When a condition spans old and new rows or a trigger needs access to both old and new, the transition tables are automatically joined on primary key.

!!! danger

    If your primary keys are updated, the join may filter them out and they won't be returned in the SQL fragments. Always keep this in mind when writing conditional statement-level triggers. Although it is rare that primary keys are updated, consider making a protection trigger for this case or by avoiding writing a conditional statement-level trigger altogether.

## Statement-level `Protect` and `ReadOnly` triggers

[pgtrigger.Composer][] makes it easier for trigger authors to define trigger classes that can conditionally execute for both statement and row-level execution. [django-pghistory](https://django-pghistory.readthedocs.io/), for example, uses [pgtrigger.Composer][] to enable both statement and row-level history tracking triggers.

[pgtrigger.Protect][] and [pgtrigger.ReadOnly][] also use [pgtrigger.Composer][], providing statement-level versions of these triggers:

```python
class MyModel(models.Model):
    class Meta:
        triggers = [
            pgtrigger.Protect(
                name="protect_updates",
                level=pgtrigger.Statement,
                operation=pgtrigger.Update
            )
        ]
```

## Performance

You may be wondering, why even use the statement-level versions of triggers or use [pgtrigger.Composer][]? It all comes down to performance.

If your application is doing large bulk updates or inserts of tables, even simple row-level protection triggers are called for every row and can show up in performance measurements. Statement-level versions can be substantially faster.

If not doing conditional triggers or doing conditional triggers where primary keys don't change, statement-level triggers can be much better for use cases where performance is key.

Always profile results yourself. [EXPLAIN ANALYZE](https://www.postgresql.org/docs/current/sql-explain.html) will show trigger overhead.
