import datetime
import itertools as it
import typing as ty
import math

from termcolor import cprint
import pandas as pd
import peewee as pw

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
        case pw.TextField():
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


def download_statcast_day(date: datetime.date) -> pd.DataFrame:
    df = statcast(start_dt=str(date), end_dt=str(date))
    assert isinstance(df, pd.DataFrame)
    return df


def download_statcast(
    models: model.DBModels, start_date: datetime.date, end_date: datetime.date
) -> None | pd.DataFrame:
    if end_date < start_date:
        raise ValueError(
            f"start_date({start_date!s}) must be the same or before end_date({end_date!s})"
        )

    cached_dates: set[datetime.date] = set()
    for record in models.DateCache.select():
        cached_date = record.date
        assert isinstance(cached_date, datetime.date)
        cached_dates.add(cached_date)

    df: pd.DataFrame | None = None
    current_date = start_date
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
            current_date += datetime.timedelta(days=1)
            continue

        assert list(df.columns) == list(day_df.columns), U.dbg_info(
            "Column mismatch", df=list(df.columns), day_df=list(day_df.columns)
        )
        df = pd.concat([df, day_df], ignore_index=True)

        current_date += datetime.timedelta(days=1)

    if df is None:
        return None

    assert isinstance(df, pd.DataFrame)
    return (
        df.drop(DROP_COLUMNS, axis=1)
        .sort_values(by=SORT_COLUMNS)
        .reset_index(drop=True)
    )


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
