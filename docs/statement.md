# Statement-Level Triggers

Statement-level triggers provide the ability to run triggers once-per statement. This can offer a significant performance advantage over its row-based counterpart by providing the ability to run bulk operations in response to bulk events. The downside here is:

1. Statement-level triggers cannot be conditionally executed, making it more cumbersome to do actions based on changes.
2. Statement-level triggers can only fire after an operation. They cannot alter rows in memory or cause Postgres to ignore certain operations.
3. Although they have access to old and new changes via [transition tables](https://dba.stackexchange.com/questions/177463/what-is-a-transition-table-in-postgres), there is no guaranteed ordering. In other words, one has to join old and new transition tables based on the primary key. Updates to primary keys can affect this logic.

With these caveats in mind, `django-pgtrigger` provides the [pgtrigger.Composer][] trigger to facilitate writing performant conditional statement-level triggers like row-level trigger counterparts. [pgtrigger.Composer][] also helps one express a trigger as both row- or statement-level functions, facilitating more advanced trigger definitions in both `django-pgtrigger` and third-party libraries.

Here we go over the fundamentals of how [pgtrigger.Composer][] works and how it is used by some triggers provided by `django-pgtrigger`.

## Automatic references declaration

[pgtrigger.Composer][] limits the boilerplate of statement-level triggers by automatically creating a `references` when `level=pgtrigger.Statement` is used. The `references` declaration makes `old_values` and `new_values` tables available based on the combinations of the operations provided.

For example:

- `pgtrigger.Update` makes `references=pgtrigger.References(old='old_values', new='new_values')`
- `pgtrigger.Delete` makes `references=pgtrigger.References(old='old_values')`
- `pgtrigger.Insert` makes `pgtrigger.References(new='new_values')`

Combinations of these will intersect references. `pgtrigger.UpdateOf` and `pgtrigger.Truncate` always generate null references.

## Template variables when using conditions

One can use `condition` with [pgtrigger.Composer][] statement-level triggers, which provides the following template variables in [pgtrigger.Func][]:

- **cond_old_values**: A fragmment that has the `old_values` alias filtered by the condition.
- **cond_new_values**: A fragment that has the `new_values` alias filtered by the condition.
- **cond_joined_values**: A fragment that always has both `old_values` and `new_values` aliases joined and filtered by the condition.

Use the minimum alias possible in your trigger to ensure best performance of generated SQL. If your condition, for example, doesn't need to join across old and new rows and you only need access to the old rows, `cond_old_values` will only filter across old rows and won't do a join.

Here's an example of a statement-level trigger that loops through all old values that match a condition in an update:

```python
pgtrigger.Composer(
    name="composer_protect",
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

[pgtrigger.Protect][] and [pgtrigger.ReadOnly][] use [pgtrigger.Composer][], providing statement-level versions of these triggers:

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
