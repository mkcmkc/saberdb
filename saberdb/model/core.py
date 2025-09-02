import dataclasses

import peewee as pw

from .date_cache import _DateCache, date_cache_model
from .game import _Game, game_model
from .player import _Player, player_model
from .pitch import _Pitch, pitch_model


@dataclasses.dataclass(frozen=True)
class DBModels:
    DateCache: type[_DateCache]
    Game: type[_Game]
    Player: type[_Player]
    Pitch: type[_Pitch]


def get_db_models(db: pw.SqliteDatabase) -> DBModels:
    return DBModels(
        DateCache=date_cache_model(db),
        Game=game_model(db),
        Player=player_model(db),
        Pitch=pitch_model(db),
    )
