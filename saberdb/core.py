import datetime

from termcolor import cprint
import pandas as pd

from pybaseball import statcast  # type: ignore

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
