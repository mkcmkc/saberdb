import typing as ty

import peewee as pw


class _DateCache(pw.Model):
    date = pw.DateField(primary_key=True, index=True)

    class Meta:
        table_name = 'date_cache'


def date_cache_model(db: pw.SqliteDatabase) -> ty.Type[_DateCache]:
    class DateCache(_DateCache):
        class Meta:  # type: ignore
            table_name = 'date_cache'
            database = db

    return DateCache
