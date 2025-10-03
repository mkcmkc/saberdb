from collections.abc import Generator
import datetime
import itertools as it
import typing as ty
import math
from pathlib import Path
import json

from termcolor import cprint
import pandas as pd
import peewee as pw
import numpy as np

from pybaseball import statcast, playerid_reverse_lookup  # type: ignore

from . import util as U
from . import model


SORT_COLUMNS = ["game_pk", "at_bat_number", "pitch_number"]
GAME_COLUMNS = [
    "game_pk",
    "game_date",
    "game_type",
    "home_team",
    "away_team",
]

DROP_COLUMNS = [
    "spin_dir",
    "spin_rate_deprecated",
    "break_angle_deprecated",
    "break_length_deprecated",
    "game_year",
    "tfs_deprecated",
    "tfs_zulu_deprecated",
    "umpire",
    "sv_id",
    "player_name",
]

DEFAULT_BATCH_SIZE_DAYS = 30


def is_null(x: ty.Any) -> bool:
    return x is None or (isinstance(x, float) and math.isnan(x))


def coerce(field: pw.Field, value: ty.Any) -> tuple[ty.Any, type]:
    new_value = value
    match field:
        case pw.BigIntegerField():
            if isinstance(value, float):
                assert value == int(value)
                new_value = int(value)

            return new_value, int
        case pw.DoubleField():
            return new_value, float
        case pw.CharField() | pw.TextField():
            return new_value, str
        case pw.DateField():
            if isinstance(value, float):
                assert value == int(value)
                new_value = int(value)

            if isinstance(new_value, int):
                new_value = datetime.datetime(new_value, 1, 1)

            return new_value, datetime.datetime
        case _:
            assert False, U.dbg_info("Cannot handle field type", field=field)


def fill_player_table(
    df: pd.DataFrame, models: model.DBModels
) -> dict[int, model._Player]:
    player_ids: set[int] = set()
    player_ids.update(set(map(int, df["batter"])))
    player_ids.update(set(map(int, df["pitcher"])))
    for player_id in filter(
        lambda x: not is_null(x), it.chain(df["on_1b"], df["on_2b"], df["on_3b"])
    ):
        assert isinstance(player_id, int) or (
            isinstance(player_id, float) and player_id == int(player_id)
        )
        player_ids.add(int(player_id))

    for i in range(2, 10):
        key = f"fielder_{i!s}"
        player_ids.update(set(map(int, df[key])))

    df_players = playerid_reverse_lookup(list(player_ids), key_type="mlbam")  # type: ignore
    player_fields: list[pw.Field] = list(models.Player._meta.fields.values())  # type: ignore
    player_lookup: dict[int, model._Player] = {}
    for _, row in df_players.iterrows():
        player_args: dict[str, ty.Any] = {}
        for field in player_fields:
            column_name = field.column_name
            assert isinstance(column_name, str)
            is_nullable: bool = column_name in {"key_retro", "key_bbref"}
            index = {
                "mlb_first_played_year": "mlb_played_first",
                "mlb_last_played_year": "mlb_played_last",
            }.get(column_name, column_name)
            assert isinstance(index, str)

            value = row[index]
            value = None if is_null(value) else value
            assert is_nullable or value is not None, U.dbg_info(
                "Cannot use `None` in non-nullable field", row=row, field=field
            )

            value, expected_type = coerce(field, value)
            assert value is None or isinstance(value, expected_type), U.dbg_info(
                "Value does not match expected type",
                row=row,
                field=field,
                value=value,
                value_type=type(value),
                is_nullable=is_nullable,
                expected_type=expected_type,
            )
            player_args[column_name] = value

        player = models.Player(**player_args)
        try:
            existing_player = models.Player.get_by_id(player.get_id())
        except pw.DoesNotExist:
            existing_player = None
            player.save(force_insert=True)

        if existing_player is not None:
            player = existing_player

        player_id = player_args["key_mlbam"]
        assert isinstance(player_id, int)
        player_lookup[player_id] = player

    missing_ids = player_ids - set(player_lookup.keys())
    df_missing_players = playerid_reverse_lookup(list(missing_ids), key_type="mlbam")  # type: ignore
    assert df_missing_players.shape[0] == 0, U.dbg_info(
        "Missing player ids are actually present", df_missing_players=df_missing_players
    )

    for player_id in missing_ids:
        assert player_id not in player_lookup

        player_args = {}
        for field in player_fields:
            column_name = field.column_name
            assert isinstance(column_name, str)
            player_args[column_name] = None

        assert "key_mlbam" in player_args
        player_args["key_mlbam"] = player_id

        player = models.Player(**player_args)
        try:
            existing_player = models.Player.get_by_id(player.get_id())
        except pw.DoesNotExist:
            existing_player = None
            player.save(force_insert=True)

        if existing_player is not None:
            player = existing_player

        assert isinstance(player_id, int)
        player_lookup[player_id] = player

    return player_lookup


# TODO(mkcmkc): Separate into individual fill functions to aggregate SQL queries.
# TODO(mkcmkc): Do in a DB transaction.
# TODO(mkcmkc): Do bulk actions to minimize SQL calls.
def fill_db(db: pw.SqliteDatabase, models: model.DBModels, df: pd.DataFrame) -> None:
    if db.is_closed():
        raise ValueError("db mus be connected")

    db.create_tables([models.Game, models.Pitch, models.Player, models.DateCache])
    cached_dates: set[str] = set()
    for record in models.DateCache.select():
        cached_date = record.date
        assert isinstance(cached_date, datetime.date)
        cached_dates.add(cached_date.strftime("%Y-%m-%d"))

    df_new = df[~(df["game_date"].isin(cached_dates))]
    player_lookup = fill_player_table(df_new, models)

    pitch_fields: list[pw.Field] = list(models.Pitch._meta.fields.values())  # type: ignore

    game_groups = df_new.groupby(["game_pk"], sort=False, as_index=False)
    player_id_fields = (
        {"batter_id", "pitcher_id"}
        | {f"on_{i!s}b_id" for i in range(1, 4)}
        | {f"fielder_{i!s}_id" for i in range(2, 10)}
    )
    for _, df_group in game_groups:
        first_row = df_group.iloc[0]
        pk = first_row["game_pk"]
        assert isinstance(pk, np.int64)  # type: ignore
        pk = int(pk)
        date_str = first_row["game_date"]
        assert isinstance(date_str, str)
        date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        game_type = first_row["game_type"]
        assert isinstance(game_type, str)
        assert game_type in model.GameType, U.dbg_info(
            "Game type is invalid", game_type=game_type
        )
        home_team = first_row["home_team"]
        assert isinstance(home_team, str)
        away_team = first_row["away_team"]
        assert isinstance(away_team, str)

        date_cache = models.DateCache(date=date)
        try:
            models.DateCache.get_by_id(date_cache.get_id())
            raise ValueError(
                "Date of game is already in DateCache: "
                + json.dumps(dict(df_group=df_group))
            )
        except pw.DoesNotExist:
            date_cache.save(force_insert=True)

        game = models.Game(
            pk=pk,
            date=date,
            game_type=game_type,
            home_team=home_team,
            away_team=away_team,
        )

        try:
            models.Game.get_by_id(game.get_id())
            raise ValueError(
                "Inserting duplicate rows into Game table: " + json.dumps(dict(pk=pk))
            )
        except pw.DoesNotExist:
            game.save(force_insert=True)

        for _, row in df_group.iterrows():
            pitch_args: dict[str, ty.Any] = {}
            for field in pitch_fields:
                is_nullable = field.null
                column_name = field.column_name
                if column_name == "id":
                    continue

                assert not isinstance(field, pw.AutoField)

                if column_name == "game_id":
                    pitch_args[column_name] = game
                    continue

                if column_name in player_id_fields:
                    if column_name in {"on_1b_id", "on_2b_id", "on_3b_id"}:
                        assert is_nullable
                    else:
                        assert not is_nullable

                    index = column_name[:-(len("_id"))]
                    player_id = row[index]
                    assert isinstance(player_id, int) or isinstance(player_id, float)
                    player_id = None if is_null(player_id) else player_id
                    if player_id is None:
                        player = None
                    else:
                        if isinstance(player_id, float):
                            assert player_id == int(player_id)  # type: ignore
                            player_id = int(player_id)

                        assert isinstance(player_id, int)
                        player = player_lookup[player_id]

                    assert is_nullable or player is not None, U.dbg_info(
                        "Cannot use `None` in non-nullable field",
                        row=row,
                        field=field,
                    )
                    pitch_args[column_name] = player
                    continue

                assert not isinstance(field, pw.ForeignKeyField)

                index = {
                    "result": "type",
                }.get(column_name, column_name)
                assert isinstance(index, str)

                if index == "half_inning":
                    inning = row["inning"]
                    assert isinstance(inning, int)
                    inning_topbot = row["inning_topbot"].lower()
                    assert inning_topbot in {"top", "bot"}
                    value: ty.Any = 2 * inning - 1
                    if inning_topbot == "bot":
                        value += 1
                else:
                    value = row[index]

                value = None if is_null(value) else value
                assert is_nullable or value is not None, U.dbg_info(
                    "Cannot use `None` in non-nullable field", row=row, field=field
                )
                value, expected_type = coerce(field, value)
                assert value is None or isinstance(value, expected_type), U.dbg_info(
                    "Invalid value/type",
                    row=row,
                    field=field,
                    value=value,
                    value_type=type(value),
                    expected_type=expected_type,
                )
                pitch_args[column_name] = value

            pitch = models.Pitch(**pitch_args)
            pitch.save(force_insert=True)


def download_statcast_day(date: datetime.date) -> pd.DataFrame:
    df = statcast(start_dt=str(date), end_dt=str(date))
    assert isinstance(df, pd.DataFrame)
    return df


def download_statcast(
    models: model.DBModels,
    start_date: datetime.date,
    end_date: datetime.date,
    batch_size_days: (None | int) = None,
) -> Generator[pd.DataFrame]:
    if end_date < start_date:
        raise ValueError(
            f"start_date({start_date!s}) must be the same or before end_date({end_date!s})"
        )

    batch_size = datetime.timedelta(
        days=(DEFAULT_BATCH_SIZE_DAYS if batch_size_days is None else batch_size_days)
    )
    cached_dates: set[datetime.date] = set()
    for record in models.DateCache.select():
        cached_date = record.date
        assert isinstance(cached_date, datetime.date)
        cached_dates.add(cached_date)

    df: pd.DataFrame | None = None
    current_date = start_date
    current_batch_size = 0
    while current_date <= end_date:
        if current_date in cached_dates:
            cprint(f"Date {current_date!s} cached... Skipping...", "green")
            continue

        print("Downloading games played on ", end="")
        cprint(current_date, "blue", attrs=["bold"])
        with U.supress_output():
            day_df = download_statcast_day(current_date)

        if day_df.shape[0] == 0:
            current_date += datetime.timedelta(days=1)
            continue

        if df is None:
            df = day_df
        else:
            assert list(df.columns) == list(day_df.columns), U.dbg_info(
                "Column mismatch", df=list(df.columns), day_df=list(day_df.columns)
            )
            df = pd.concat([df, day_df], ignore_index=True)

        current_date += datetime.timedelta(days=1)
        current_batch_size += 1
        if current_batch_size == batch_size.days:
            current_batch_size = 0
            assert isinstance(df, pd.DataFrame)
            yield (
                df.drop(DROP_COLUMNS, axis=1)
                .sort_values(by=SORT_COLUMNS)
                .reset_index(drop=True)
            )
            df = None

    if df is not None:
        assert isinstance(df, pd.DataFrame)
        yield (
            df.drop(DROP_COLUMNS, axis=1)
            .sort_values(by=SORT_COLUMNS)
            .reset_index(drop=True)
        )


def download_into_db(db_path: Path, start_date: datetime.date, end_date: datetime.date):
    db: None | pw.SqliteDatabase = None
    try:
        db = pw.SqliteDatabase(str(db_path))
        models = model.get_db_models(db)
        db.connect()
        for df in download_statcast(models, start_date, end_date):
            fill_db(db, models, df)
    finally:
        if db is not None and not db.is_closed():
            db.close()
