import enum
import typing as ty

import peewee as pw


def enum_to_field(en: ty.Type[enum.StrEnum], *, null: bool=False) -> (pw.FixedCharField | pw.CharField):
    choices = [(x.value, x.name) for x in en]
    max_len = max(len(x) for x, _ in choices)
    assert max_len >= 1
    if max_len == 1:
        return pw.FixedCharField(choices=choices, null=null)

    return pw.CharField(choices=choices, max_length=max_len, null=null)
