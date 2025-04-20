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
def test_custom_db_table_protect_trigger():
    """Verify custom DB table names have successful triggers"""
    deletion_protected_model = ddf.G(models.CustomTableName)
    with utils.raises_trigger_error(match="Cannot delete rows"):
        deletion_protected_model.delete()


@pytest.mark.django_db
def test_cond_values_protect():
    """Verify cond_values trigger with protection-like trigger."""

    # Test a simple protection trigger that loops through conditional rows
    cond_values_raise = pgtrigger.CondValues(
        name="cond_values_protect",
        when=pgtrigger.After,
        operation=pgtrigger.Insert,
        declare=[("val", "RECORD")],
        func=pgtrigger.Func(
            """
            FOR val IN {cond_new_values} LOOP RAISE EXCEPTION 'hit condition'; END LOOP;
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
def test_cond_values_protect_no_condition():
    """Verify cond_values trigger with protection-like trigger."""

    # Test a simple protection trigger that loops through conditional rows
    cond_values_raise = pgtrigger.CondValues(
        name="cond_values_protect",
        when=pgtrigger.After,
        operation=pgtrigger.Insert,
        declare=[("val", "RECORD")],
        func=pgtrigger.Func(
            """
            FOR val IN {cond_new_values} LOOP RAISE EXCEPTION 'hit condition'; END LOOP;
            RETURN NULL;
            """
        ),
    )

    with cond_values_raise.install(models.TestTrigger):
        with utils.raises_trigger_error(match="hit condition"):
            models.TestTrigger.objects.create(int_field=2)


@pytest.mark.django_db
def test_cond_values_protect_custom_condition():
    """Verify cond_values trigger with a custom protection-like condition."""

    cond_values_raise = pgtrigger.CondValues(
        name="cond_values_protect_update",
        when=pgtrigger.After,
        operation=pgtrigger.Update,
        declare=[("val", "RECORD")],
        func=pgtrigger.Func(
            """
            FOR val IN {cond_new_values} LOOP RAISE EXCEPTION 'hit condition'; END LOOP;
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
def test_cond_values_log():
    """Verify cond_values trigger works on test model"""
    cond_values_log = pgtrigger.CondValues(
        name="cond_values_log",
        when=pgtrigger.After,
        operation=pgtrigger.Update,
        declare=[("val", "RECORD")],
        func=pgtrigger.Func(
            f"""
            INSERT INTO {models.TestTrigger._meta.db_table} (field, int_field, dt_field)
            SELECT new_values.field, new_values.int_field, old_values.dt_field
            FROM {{cond_from}};
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


def test_cond_values_properties():
    """Verify CondValues trigger properties."""
    with pytest.raises(ValueError, match="single Insert"):
        pgtrigger.CondValues(
            name="cond_values_properties",
            when=pgtrigger.After,
            operation=pgtrigger.Update | pgtrigger.Insert,
        )

    with pytest.raises(ValueError, match="referencing"):
        pgtrigger.CondValues(
            name="cond_values_properties",
            when=pgtrigger.After,
            operation=pgtrigger.Update,
            referencing=pgtrigger.Referencing(new="new_values"),
        )

    insert_cond_values = pgtrigger.CondValues(
        name="cond_values_properties",
        when=pgtrigger.After,
        operation=pgtrigger.Insert,
    )
    assert insert_cond_values.referencing.new == "new_values"
    assert insert_cond_values.referencing.old is None

    insert_cond_values = pgtrigger.CondValues(
        name="cond_values_properties",
        when=pgtrigger.After,
        operation=pgtrigger.Update,
    )
    assert insert_cond_values.referencing.new == "new_values"
    assert insert_cond_values.referencing.old == "old_values"

    delete_cond_values = pgtrigger.CondValues(
        name="cond_values_properties",
        when=pgtrigger.After,
        operation=pgtrigger.Delete,
    )
    assert delete_cond_values.referencing.new is None
    assert delete_cond_values.referencing.old == "old_values"

    with pytest.raises(ValueError, match="OLD values"):
        pgtrigger.CondValues(
            name="cond_values_properties",
            when=pgtrigger.After,
            operation=pgtrigger.Insert,
            condition=pgtrigger.Q(old__int_field__gt=0),
            func=pgtrigger.Func("RETURN NULL;"),
        ).render_func(models.TestTrigger)

    with pytest.raises(ValueError, match="NEW values"):
        pgtrigger.CondValues(
            name="cond_values_properties",
            when=pgtrigger.After,
            operation=pgtrigger.Delete,
            condition=pgtrigger.Q(new__int_field__gt=0),
            func=pgtrigger.Func("RETURN NULL;"),
        ).render_func(models.TestTrigger)
