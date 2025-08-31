import enum
import typing as ty

import peewee as pw

from . import util


class GameType(enum.StrEnum):
    EXHIBITION = "E"
    SPRING_TRAINING = "S"
    REGULAR_SEASON = "R"
    WILD_CARD = "F"
    DIVISIONAL_SERIES = "D"
    LEAGUE_CHAMPIONSHIP_SERIES = "L"
    WORLD_SERIES = "W"


# TODO(mkcmkc): Make date a foreign key into DateCache.
class _Game(pw.Model):
    pk = pw.PrimaryKeyField()
    date = pw.DateField(index=True)
    game_type = util.enum_to_field(GameType)
    home_team = pw.TextField()
    away_team = pw.TextField()

    class Meta:
        table_name = "game"


def game_model(db: pw.SqliteDatabase) -> ty.Type[_Game]:
    class Game(_Game):
        class Meta:  # type: ignore
            table_name = "game"
            database = db

    return Game
