import datetime as dt

import ddf
import django
import pgbulk
import pytest
from django.core.exceptions import FieldDoesNotExist

import pgtrigger
from pgtrigger.tests import models, utils


def test_registered_invalid_args():
    with pytest.raises(ValueError):
        pgtrigger.registered("uri")


@pytest.mark.django_db
def test_read_only():
    """Tests the ReadOnly trigger"""
    with pytest.raises(ValueError, match="only one of"):
        pgtrigger.ReadOnly(name="uneditable", fields=["level"], exclude=["hello"])

    trigger = pgtrigger.ReadOnly(name="uneditable", fields=["level"])
    with pytest.raises(FieldDoesNotExist):
        trigger.install(models.TestModel)

    trigger = pgtrigger.ReadOnly(name="uneditable", exclude=["level"])
    with pytest.raises(FieldDoesNotExist):
        trigger.install(models.TestModel)

    trigger = pgtrigger.ReadOnly(name="uneditable")
    with trigger.install(models.TestModel):
        m = ddf.G(models.TestModel, int_field=1)
        m.save()

        with utils.raises_trigger_error(match="Cannot update rows"):
            m.int_field = 2
            m.save()

    trigger = pgtrigger.ReadOnly(name="uneditable", fields=["char_field", "float_field"])
    with trigger.install(models.TestModel):
        m = ddf.G(models.TestModel, int_field=2, char_field="a")
        m.int_field = 3
        m.save()

        with utils.raises_trigger_error(match="Cannot update rows"):
            m.char_field = "b"
            m.save()

    trigger = pgtrigger.ReadOnly(name="uneditable", exclude=["int_field"])
    with trigger.install(models.TestModel):
        m = ddf.G(models.TestModel, int_field=4, char_field="a")
        m.int_field = 5
        m.save()

        with utils.raises_trigger_error(match="Cannot update rows"):
            m.char_field = "b"
            m.save()


@pytest.mark.django_db
def test_search_model():
    """Verifies search model fields are kept up to date"""
    obj = models.SearchModel.objects.create(
        title="This is a message", body="Hello World. What a great body."
    )
    models.SearchModel.objects.create(title="Hi guys", body="Random Word. This is a good idea.")
    models.SearchModel.objects.create(
        title="Hello", body="Other words. Many great ideas come from stuff."
    )
    models.SearchModel.objects.create(title="The title", body="A short message.")

    assert models.SearchModel.objects.filter(body_vector="hello").count() == 1
    assert models.SearchModel.objects.filter(body_vector="words").count() == 2
    assert models.SearchModel.objects.filter(body_vector="world").count() == 1
    assert models.SearchModel.objects.filter(title_body_vector="message").count() == 2
    assert models.SearchModel.objects.filter(title_body_vector="idea").count() == 2
    assert models.SearchModel.objects.filter(title_body_vector="hello").count() == 2

    obj.body = "Nothing more"
    obj.save()
    assert not models.SearchModel.objects.filter(body_vector="hello").exists()
    assert models.SearchModel.objects.filter(title_body_vector="hello").count() == 1


def test_update_search_vector_args():
    """Verifies arg checking for UpdateSearchVector"""
    with pytest.raises(ValueError, match='provide "vector_field"'):
        pgtrigger.UpdateSearchVector()

    with pytest.raises(ValueError, match='provide "document_fields"'):
        pgtrigger.UpdateSearchVector(vector_field="vector_field")


def test_update_search_vector_ignore():
    """Verifies UpdateSearchVector cannot be ignored"""
    trigger = pgtrigger.UpdateSearchVector(
        name="hi", vector_field="vector_field", document_fields=["hi"]
    )
    with pytest.raises(RuntimeError, match="Cannot ignore UpdateSearchVector"):
        with trigger.ignore(models.SearchModel):
            pass


@pytest.mark.django_db
def test_soft_delete():
    """
    Verifies the SoftDelete test model has the "is_active" flag set to false
    """
    soft_delete = ddf.G(models.SoftDelete, is_active=True)
    ddf.G(models.FkToSoftDelete, ref=soft_delete)
    soft_delete.delete()

    assert not models.SoftDelete.objects.get().is_active
    assert not models.FkToSoftDelete.objects.exists()


@pytest.mark.django_db
def test_soft_delete_composite_pk():
    """
    Verifies the SoftDelete test model has the "is_active" flag set to false
    """
    if django.VERSION >= (5, 2):
        models.SoftDeleteCompositePk.objects.create(is_active=True, id_1=1, id_2=1)
        soft_delete = models.SoftDeleteCompositePk.objects.create(is_active=True, id_1=2, id_2=2)
        soft_delete.delete()

        assert models.SoftDeleteCompositePk.objects.get(id_1=1, id_2=1).is_active
        assert not models.SoftDeleteCompositePk.objects.get(id_1=2, id_2=2).is_active


@pytest.mark.django_db
def test_customer_soft_delete():
    """
    Verifies the CustomSoftDelete test model has the "custom_active" flag set
    to false
    """
    soft_delete = ddf.G(models.CustomSoftDelete, custom_active=True)
    soft_delete.delete()

    assert not models.CustomSoftDelete.objects.get().custom_active


@pytest.mark.django_db
def test_soft_delete_different_values():
    """
    Tests SoftDelete with different types of fields and values
    """
    # Make the LogEntry model a soft delete model where
    # "level" is set to "inactive"
    trigger = pgtrigger.SoftDelete(name="soft_delete", field="level", value="inactive")
    with trigger.install(models.LogEntry):
        le = ddf.G(models.LogEntry, level="active")
        le.delete()
        assert models.LogEntry.objects.get().level == "inactive"
    models.LogEntry.objects.all().delete()

    # Make the LogEntry model a soft delete model where
    # "old_field" is set to None
    trigger = pgtrigger.SoftDelete(name="soft_delete", field="old_field", value=None)
    with trigger.install(models.LogEntry):
        le = ddf.G(models.LogEntry, old_field="something")
        le.delete()
        assert models.LogEntry.objects.get().old_field is None


@pytest.mark.django_db
def test_fsm():
    """
    Verifies the FSM test model cannot make invalid transitions
    """
    fsm = ddf.G(models.FSM, transition="unpublished")
    fsm.transition = "inactive"
    with utils.raises_trigger_error(match="Invalid transition"):
        fsm.save()

    fsm.transition = "published"
    fsm.save()

    # Be sure we ignore FSM when there is no transition
    fsm.save()

    with utils.raises_trigger_error(match="Invalid transition"):
        fsm.transition = "unpublished"
        fsm.save()

    fsm.transition = "inactive"
    fsm.save()


def test_fsm_args():
    """Verifies arg checking for FSM"""
    with pytest.raises(ValueError, match='provide "field"'):
        pgtrigger.FSM()

    with pytest.raises(ValueError, match='provide "transitions"'):
        pgtrigger.FSM(field="hello")

    with pytest.raises(ValueError, match='contains separator ":"'):
        pgtrigger.FSM(field="hello", transitions=[("a", ":")])

    with pytest.raises(ValueError, match='contains separator ","'):
        pgtrigger.FSM(field="hello", separator=",", transitions=[("a", ",")])

    with pytest.raises(ValueError, match="contains quotes"):
        pgtrigger.FSM(field="hello", transitions=[("a", "b'")])

    with pytest.raises(ValueError, match="contains quotes"):
        pgtrigger.FSM(field="hello", transitions=[("a", 'b"')])

    with pytest.raises(ValueError, match="single character"):
        pgtrigger.FSM(field="hello", separator="aa", transitions=[("a", "b")])

    with pytest.raises(ValueError, match="must not have quotes"):
        pgtrigger.FSM(field="hello", separator="'", transitions=[("a", "b")])


@pytest.mark.django_db
def test_protect():
    """Verify deletion protect trigger works on test model"""
    deletion_protected_model = ddf.G(models.TestTrigger)
    with utils.raises_trigger_error(match="Cannot delete rows"):
        deletion_protected_model.delete()


@pytest.mark.django_db
def test_protect_statement_level_insert_update():
    """Verify insert/update protect trigger works on test model"""
    cond_protect = pgtrigger.Protect(
        name="cond_values_protect",
        operation=pgtrigger.Insert,
        level=pgtrigger.Statement,
        condition=pgtrigger.Q(new__int_field__gt=100),
    )

    with cond_protect.install(models.TestTrigger):
        models.TestTrigger.objects.bulk_create(
            [
                models.TestTrigger(int_field=2),
                models.TestTrigger(int_field=20),
                models.TestTrigger(int_field=30),
            ]
        )
        with utils.raises_trigger_error(match="Cannot insert rows"):
            models.TestTrigger.objects.bulk_create(
                [
                    models.TestTrigger(int_field=2),
                    models.TestTrigger(int_field=200),
                ]
            )

    cond_protect = pgtrigger.Protect(
        name="cond_values_protect",
        operation=pgtrigger.Update,
        level=pgtrigger.Statement,
        condition=pgtrigger.Q(new__int_field__gt=100, old__int_field__lte=100),
    )

    with cond_protect.install(models.TestTrigger):
        models.TestTrigger.objects.bulk_create(
            [
                models.TestTrigger(int_field=2),
                models.TestTrigger(int_field=20),
                models.TestTrigger(int_field=30),
            ]
        )
        models.TestTrigger.objects.update(int_field=1)
        with utils.raises_trigger_error(match="Cannot update rows"):
            models.TestTrigger.objects.update(int_field=101)


@pytest.mark.django_db
def test_protect_statement_level_delete():
    """Verify deletion protect trigger works on test model"""
    cond_protect = pgtrigger.Protect(
        name="cond_values_protect",
        operation=pgtrigger.Delete,
        level=pgtrigger.Statement,
        condition=pgtrigger.Q(old__int_field__gt=20),
    )

    with (
        cond_protect.install(models.TestTrigger),
        pgtrigger.ignore("tests.TestTriggerProxy:protect_delete"),
    ):
        values = models.TestTrigger.objects.bulk_create(
            [
                models.TestTrigger(int_field=2),
                models.TestTrigger(int_field=20),
                models.TestTrigger(int_field=30),
            ]
        )
        print("values", values[0].int_field)
        values[0].delete()
        with utils.raises_trigger_error(match="Cannot delete rows"):
            models.TestTrigger.objects.all().delete()


@pytest.mark.django_db
def test_readonly_statement_level():
    """Verify readonly statement protect trigger works on test model"""
    cond_protect = pgtrigger.ReadOnly(
        name="cond_values_protect",
        level=pgtrigger.Statement,
        fields=["int_field"],
    )

    with cond_protect.install(models.TestTrigger):
        models.TestTrigger.objects.bulk_create(
            [
                models.TestTrigger(int_field=2),
                models.TestTrigger(int_field=20),
                models.TestTrigger(int_field=30),
            ]
        )
        with utils.raises_trigger_error(match="Cannot update rows"):
            models.TestTrigger.objects.update(int_field=101)


@pytest.mark.django_db
def test_custom_db_table_protect_trigger():
    """Verify custom DB table names have successful triggers"""
    deletion_protected_model = ddf.G(models.CustomTableName)
    with utils.raises_trigger_error(match="Cannot delete rows"):
        deletion_protected_model.delete()


@pytest.mark.django_db
def test_composer_protect():
    """Verify composer trigger with protection-like trigger."""

    # Test a simple protection trigger that loops through conditional rows
    cond_values_raise = pgtrigger.Composer(
        name="cond_values_protect",
        when=pgtrigger.After,
        operation=pgtrigger.Insert,
        level=pgtrigger.Statement,
        declare=[("val", "RECORD")],
        func=pgtrigger.Func(
            """
            FOR val IN SELECT * FROM {cond_new_values}
            LOOP
            RAISE EXCEPTION 'hit condition';
            END LOOP;
            RETURN NULL;
            """
        ),
        condition=pgtrigger.Q(new__int_field__gt=0),
    )

    with cond_values_raise.install(models.TestTrigger):
        ddf.G(models.TestTrigger, int_field=0)
        with utils.raises_trigger_error(match="hit condition"):
            models.TestTrigger.objects.create(int_field=2)


@pytest.mark.django_db
def test_composer_protect_no_condition():
    """Verify cond_values trigger with protection-like trigger."""

    # Test a simple protection trigger that loops through conditional rows
    cond_values_raise = pgtrigger.Composer(
        name="cond_values_protect",
        when=pgtrigger.After,
        operation=pgtrigger.Insert,
        level=pgtrigger.Statement,
        declare=[("val", "RECORD")],
        func=pgtrigger.Func(
            """
            FOR val IN SELECT * FROM {cond_new_values}
            LOOP
            RAISE EXCEPTION 'hit condition';
            END LOOP;
            RETURN NULL;
            """
        ),
    )

    with cond_values_raise.install(models.TestTrigger):
        with utils.raises_trigger_error(match="hit condition"):
            models.TestTrigger.objects.create(int_field=2)


@pytest.mark.django_db
def test_composer_protect_custom_condition():
    """Verify composer trigger with a custom protection-like condition."""

    cond_values_raise = pgtrigger.Composer(
        name="cond_values_protect_update",
        when=pgtrigger.After,
        operation=pgtrigger.Update,
        level=pgtrigger.Statement,
        declare=[("val", "RECORD")],
        func=pgtrigger.Func(
            """
            FOR val IN SELECT * FROM {cond_new_values}
            LOOP
            RAISE EXCEPTION 'hit condition';
            END LOOP;
            RETURN NULL;
            """
        ),
        condition=pgtrigger.Condition("NEW.* IS DISTINCT FROM OLD.*"),
    )

    with cond_values_raise.install(models.TestTrigger):
        ddf.G(models.TestTrigger, int_field=0, n=5)
        # A redundant update should not trigger
        models.TestTrigger.objects.update(int_field=0)

        with utils.raises_trigger_error(match="hit condition"):
            models.TestTrigger.objects.update(int_field=2)


@pytest.mark.django_db
def test_composer_log():
    """Verify composer trigger works on test model"""
    cond_values_log = pgtrigger.Composer(
        name="cond_values_log",
        when=pgtrigger.After,
        operation=pgtrigger.Update,
        level=pgtrigger.Statement,
        declare=[("val", "RECORD")],
        func=pgtrigger.Func(
            f"""
            INSERT INTO {models.TestTrigger._meta.db_table} (field, int_field, dt_field)
            SELECT new_values.field, new_values.int_field, old_values.dt_field
            FROM {{cond_joined_values}};
            RETURN NULL;
            """
        ),
        condition=pgtrigger.Q(new__int_field__gt=0, old__int_field__lte=100),
    )

    with cond_values_log.install(models.TestTrigger):
        assert models.TestTrigger.objects.count() == 0
        ddf.G(models.TestTrigger, int_field=0, field="a", dt_field=dt.datetime(2015, 1, 1), n=5)
        assert models.TestTrigger.objects.count() == 5

        # The conditional statement trigger should not fire
        models.TestTrigger.objects.update(int_field=-1)
        assert models.TestTrigger.objects.count() == 5

        # The condition will fire this time, creating 5 new rows
        models.TestTrigger.objects.update(
            int_field=101, dt_field=dt.datetime(2016, 1, 2), field="b"
        )
        assert models.TestTrigger.objects.count() == 10
        # Verify the values of the last six rows
        for row in models.TestTrigger.objects.order_by("id")[6:]:
            assert row.int_field == 101
            assert row.field == "b"
            assert row.dt_field == dt.datetime(2015, 1, 1)

        # Updating should not trigger
        models.TestTrigger.objects.update(int_field=1)
        assert models.TestTrigger.objects.count() == 10

        # Do a bulk update with different values
        test_triggers = list(models.TestTrigger.objects.order_by("id"))
        # These five should not trigger
        for val in test_triggers[:5]:
            val.int_field = 0

        pgbulk.update(models.TestTrigger, test_triggers)
        assert models.TestTrigger.objects.count() == 15


@pytest.mark.parametrize(
    "operation, expected",
    [
        (pgtrigger.Insert, pgtrigger.Referencing(new="new_values")),
        (pgtrigger.Truncate, None),
        (pgtrigger.UpdateOf("field"), None),
        (pgtrigger.UpdateOf("field") | pgtrigger.Insert, None),
        (pgtrigger.Truncate | pgtrigger.Insert, None),
        (pgtrigger.Update, pgtrigger.Referencing(old="old_values", new="new_values")),
        (
            pgtrigger.Update | pgtrigger.Insert,
            pgtrigger.Referencing(new="new_values"),
        ),
        (
            pgtrigger.Update | pgtrigger.Delete,
            pgtrigger.Referencing(old="old_values"),
        ),
        (pgtrigger.Update | pgtrigger.Insert | pgtrigger.Delete, None),
        (pgtrigger.Delete, pgtrigger.Referencing(old="old_values")),
    ],
)
def test_composer_referencing(operation, expected):
    """Verify Composer trigger referencing."""
    composer = pgtrigger.Composer(
        name="composer_referencing",
        level=pgtrigger.Statement,
        when=pgtrigger.After,
        operation=operation,
    )
    assert composer.referencing == expected


@pytest.mark.parametrize(
    "condition, expected",
    [
        (None, 'old_values JOIN new_values ON (old_values."id") = (new_values."id")'),
        (
            pgtrigger.Condition("NEW.* IS DISTINCT FROM OLD.*"),
            'old_values JOIN new_values ON (old_values."id") = (new_values."id") WHERE (new_values.* IS DISTINCT FROM old_values.*)',  # noqa
        ),
        (
            pgtrigger.Q(new__int_field__gt=1, old__int_field__lte=100),
            'old_values JOIN new_values ON (old_values."id") = (new_values."id") WHERE (new_values."int_field" > 1 AND old_values."int_field" <= 100)',  # noqa
        ),
    ],
)
def test_composer_get_func_template_kwargs_cond_joined_values(condition, expected):
    """Verify Composer trigger get_func_template_kwargs for the cond_joined_values property."""
    assert (
        pgtrigger.Composer(
            name="composer_values_properties",
            level=pgtrigger.Statement,
            when=pgtrigger.After,
            operation=pgtrigger.Update,
            condition=condition,
            func=pgtrigger.Func("RETURN NULL;"),
        )
        .get_func_template_kwargs(models.TestTrigger)["cond_joined_values"]
        .strip()
        == expected
    )


@pytest.mark.parametrize(
    "condition, expected",
    [
        (None, "old_values"),
        (
            pgtrigger.Condition("NEW.* IS DISTINCT FROM OLD.*"),
            'old_values JOIN new_values ON (old_values."id") = (new_values."id") WHERE (new_values.* IS DISTINCT FROM old_values.*)',  # noqa
        ),
        (
            pgtrigger.Q(old__int_field__gt=1),
            'old_values WHERE (old_values."int_field" > 1)',
        ),
        (
            pgtrigger.Q(old__int_field__gt=1) | pgtrigger.Q(new__int_field__lte=100),
            'old_values JOIN new_values ON (old_values."id") = (new_values."id") WHERE (old_values."int_field" > 1 OR new_values."int_field" <= 100)',  # noqa
        ),
    ],
)
def test_composer_get_func_template_kwargs_cond_old_values(condition, expected):
    """Verify Composer trigger get_func_template_kwargs for the cond_old_values property."""
    assert (
        pgtrigger.Composer(
            name="composer_values_properties",
            level=pgtrigger.Statement,
            when=pgtrigger.After,
            operation=pgtrigger.Update,
            condition=condition,
            func=pgtrigger.Func("RETURN NULL;"),
        )
        .get_func_template_kwargs(models.TestTrigger)["cond_old_values"]
        .strip()
        == expected
    )


@pytest.mark.parametrize(
    "condition, expected",
    [
        (None, "new_values"),
        (
            pgtrigger.Condition("NEW.* IS DISTINCT FROM OLD.*"),
            'old_values JOIN new_values ON (old_values."id") = (new_values."id") WHERE (new_values.* IS DISTINCT FROM old_values.*)',  # noqa
        ),
        (
            pgtrigger.Q(new__int_field__gt=1),
            'new_values WHERE (new_values."int_field" > 1)',
        ),
        (
            pgtrigger.Q(old__int_field__gt=1),
            'old_values JOIN new_values ON (old_values."id") = (new_values."id") WHERE (old_values."int_field" > 1)',  # noqa
        ),
        (
            pgtrigger.Q(old__int_field__gt=1) | pgtrigger.Q(new__int_field__lte=100),
            'old_values JOIN new_values ON (old_values."id") = (new_values."id") WHERE (old_values."int_field" > 1 OR new_values."int_field" <= 100)',  # noqa
        ),
    ],
)
def test_composer_get_func_template_kwargs_cond_new_values(condition, expected):
    """Verify Composer trigger get_func_template_kwargs for the cond_new_values property."""
    assert (
        pgtrigger.Composer(
            name="composer_values_properties",
            level=pgtrigger.Statement,
            when=pgtrigger.After,
            operation=pgtrigger.Update,
            condition=condition,
            func=pgtrigger.Func("RETURN NULL;"),
        )
        .get_func_template_kwargs(models.TestTrigger)["cond_new_values"]
        .strip()
        == expected
    )


def test_composer_properties():
    """Verify Composer trigger properties."""
    with pytest.raises(ValueError, match="referencing"):
        pgtrigger.Composer(
            name="composer_values_properties",
            level=pgtrigger.Statement,
            when=pgtrigger.After,
            operation=pgtrigger.Update,
            referencing=pgtrigger.Referencing(new="new_values"),
        )

    func = pgtrigger.Func("RETURN NULL;")
    assert (
        pgtrigger.Composer(
            name="composer_values_properties",
            level=pgtrigger.Statement,
            when=pgtrigger.After,
            operation=pgtrigger.Update,
            func={
                pgtrigger.Statement: func,
            },
        ).get_func(models.TestTrigger)
        == func
    )
    assert (
        pgtrigger.Composer(
            name="composer_values_properties",
            level=pgtrigger.Row,
            when=pgtrigger.After,
            operation=pgtrigger.Update,
            func={
                pgtrigger.Row: func,
            },
        ).get_func(models.TestTrigger)
        == func
    )

    # Verify we can't render the func if it references a non-existent transition table
    with pytest.raises(ValueError, match="references OLD"):
        pgtrigger.Composer(
            name="composer_values_properties",
            level=pgtrigger.Statement,
            when=pgtrigger.After,
            operation=pgtrigger.Insert,
            func="SELECT * FROM old_values.*",
        ).render_func(models.TestTrigger)

    with pytest.raises(ValueError, match="references OLD"):
        pgtrigger.Composer(
            name="composer_values_properties",
            level=pgtrigger.Statement,
            when=pgtrigger.After,
            operation=pgtrigger.Update | pgtrigger.Insert,
            func="SELECT * FROM old_values.*",
        ).render_func(models.TestTrigger)

    with pytest.raises(ValueError, match="references OLD"):
        pgtrigger.Composer(
            name="composer_values_properties",
            level=pgtrigger.Statement,
            when=pgtrigger.After,
            operation=pgtrigger.Update | pgtrigger.Insert,
            func=pgtrigger.Func("SELECT * FROM {cond_old_values}"),
            condition=pgtrigger.AnyChange(),
        ).render_func(models.TestTrigger)

    with pytest.raises(ValueError, match="references NEW"):
        pgtrigger.Composer(
            name="composer_values_properties",
            level=pgtrigger.Statement,
            when=pgtrigger.After,
            operation=pgtrigger.Delete,
            func="SELECT * FROM new_values.*",
        ).render_func(models.TestTrigger)

    with pytest.raises(ValueError, match="references NEW"):
        pgtrigger.Composer(
            name="composer_values_properties",
            level=pgtrigger.Statement,
            when=pgtrigger.After,
            operation=pgtrigger.Update | pgtrigger.Delete,
            func="SELECT * FROM new_values.*",
        ).render_func(models.TestTrigger)

    with pytest.raises(ValueError, match="references NEW"):
        pgtrigger.Composer(
            name="composer_values_properties",
            level=pgtrigger.Statement,
            when=pgtrigger.After,
            operation=pgtrigger.Update | pgtrigger.Delete,
            func=pgtrigger.Func("SELECT * FROM {cond_new_values}"),
            condition=pgtrigger.AnyChange(),
        ).render_func(models.TestTrigger)
