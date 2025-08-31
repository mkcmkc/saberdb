import typing as ty

import peewee as pw


class _Player(pw.Model):
    key_mlbam = pw.BigIntegerField(primary_key=True)
    name_last = pw.TextField(null=True)
    name_first = pw.TextField(null=True)
    key_retro = pw.TextField(null=True)
    key_bbref = pw.TextField(null=True)
    key_fangraphs = pw.BigIntegerField(null=True)
    mlb_first_played_year = pw.DateField(null=True)
    mlb_last_played_year = pw.DateField(null=True)

    class Meta:
        table_name = 'player'


def player_model(db: pw.SqliteDatabase) -> ty.Type[_Player]:
    class Player(_Player):
        class Meta:  # type: ignore
            table_name = 'player'
            database = db

    return Player
