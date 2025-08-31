import enum
import typing as ty

import peewee as pw

from . import util
from .game import _Game
from .player import _Player


class Handedness(enum.StrEnum):
    LEFT = 'L'
    RIGHT = 'R'


class PitchResult(enum.StrEnum):
    BALL = 'B'
    STRIKE = 'S'
    HIT = 'X'


class InFieldingAlignment(enum.StrEnum):
    INFIELD_SHADE = 'Infield shade'
    STANDARD = 'Standard'
    STRATEGIC = 'Strategic'


class OutFieldingAlignment(enum.StrEnum):
    FOURTH_OUTFIELDER = '4th outfielder'
    STANDARD = 'Standard'
    STRATEGIC = 'Strategic'


class BattedBallType(enum.StrEnum):
   FLY_BALL = 'fly_ball'
   GROUND_BALL = 'ground_ball'
   LINE_DRIVE = 'line_drive'
   POPUP = 'popup'


class PitchDescription(enum.StrEnum):
    BALL = 'ball'
    BLOCKED_BALL = 'blocked_ball'
    BUNT_FOUL_TIP = 'bunt_foul_tip'
    CALLED_STRIKE = 'called_strike'
    FOUL = 'foul'
    FOUL_BUNT = 'foul_bunt'
    FOUL_TIP = 'foul_tip'
    HIT_BY_PITCH = 'hit_by_pitch'
    HIT_INTO_PLAY = 'hit_into_play'
    MISSED_BUNT = 'missed_bunt'
    PITCHOUT = 'pitchout'
    SWINGING_STRIKE = 'swinging_strike'
    SWINGING_STRIKE_BLOCKE = 'swinging_strike_blocked'


class AtBatEvent(enum.StrEnum):
    CATCHER_INTERF = 'catcher_interf'
    DOUBLE = 'double'
    DOUBLE_PLAY = 'double_play'
    FIELD_ERROR = 'field_error'
    FIELD_OUT = 'field_out'
    FIELDERS_CHOICE = 'fielders_choice'
    FIELDERS_CHOICE_OUT = 'fielders_choice_out'
    FORCE_OUT = 'force_out'
    GROUNDED_INTO_DOUBLE_PLAY = 'grounded_into_double_play'
    HIT_BY_PITCH = 'hit_by_pitch'
    HOME_RUN = 'home_run'
    SAC_BUNT = 'sac_bunt'
    SAC_FLY = 'sac_fly'
    SAC_FLY_DOUBLE_PLAY = 'sac_fly_double_play'
    SINGLE = 'single'
    STRIKEOUT = 'strikeout'
    STRIKEOUT_DOUBLE_PLAY = 'strikeout_double_play'
    TRIPLE = 'triple'
    TRIPLE_PLAY = 'triple_play'
    TRUNCATED_PA = 'truncated_pa'
    WALK = 'walk'


class PitchType(enum.StrEnum):
    FOUR_SEAM_FASTBALL = 'FF'
    CHANGEUP = 'CH'
    SLIDER = 'SL'
    CUTTER = 'FC'
    SINKER = 'SI'
    CURVEBALL = 'CU'
    KNUCKLE_CURVE = 'KC'
    SWEEPER = 'ST'
    SPLIT_FINGER = 'FS'
    SLURVE = 'SV'
    OTHER = 'FA'
    EEPHUS = 'EP'
    SLOW_CURVE = 'CS'
    SCREWBALL = 'SC'
    KNUCKLEBALL = 'KN'
    PITCH_OUT = 'PO'


# TODO(mkcmkc): Remove redundant fields.
class _Pitch(pw.Model):
    game = pw.ForeignKeyField(_Game, backref='pitches')
    pitch_type = util._enum_to_field(PitchType, null=True)
    release_speed = pw.DoubleField(null=True)
    release_pos_x = pw.DoubleField(null=True)
    release_pos_z = pw.DoubleField(null=True)
    batter = pw.ForeignKeyField(_Player, backref='as_batter')
    pitcher = pw.ForeignKeyField(_Player, backref='as_pitcher')
    events = util._enum_to_field(AtBatEvent, null=True)
    description = util._enum_to_field(PitchDescription)
    zone = pw.BigIntegerField(null=True)
    des = pw.TextField()
    stand = util._enum_to_field(Handedness)
    p_throws = util._enum_to_field(Handedness)
    result = util._enum_to_field(PitchResult)
    hit_location = pw.BigIntegerField(null=True)
    bb_type = util._enum_to_field(BattedBallType, null=True)
    balls = pw.BigIntegerField()
    strikes = pw.BigIntegerField()
    pfx_x = pw.DoubleField(null=True)
    pfx_z = pw.DoubleField(null=True)
    plate_x = pw.DoubleField(null=True)
    plate_z = pw.DoubleField(null=True)
    on_3b = pw.ForeignKeyField(_Player, null=True, backref='on_3b')
    on_2b = pw.ForeignKeyField(_Player, null=True, backref='on_2b')
    on_1b = pw.ForeignKeyField(_Player, null=True, backref='on_1b')
    outs_when_up = pw.BigIntegerField()
    half_inning = pw.BigIntegerField()
    hc_x = pw.DoubleField(null=True)
    hc_y = pw.DoubleField(null=True)
    vx0 = pw.DoubleField(null=True)
    vy0 = pw.DoubleField(null=True)
    vz0 = pw.DoubleField(null=True)
    ax = pw.DoubleField(null=True)
    ay = pw.DoubleField(null=True)
    az = pw.DoubleField(null=True)
    sz_top = pw.DoubleField(null=True)
    sz_bot = pw.DoubleField(null=True)
    hit_distance_sc = pw.BigIntegerField(null=True)
    launch_speed = pw.DoubleField(null=True)
    launch_angle = pw.BigIntegerField(null=True)
    effective_speed = pw.DoubleField(null=True)
    release_spin_rate = pw.BigIntegerField(null=True)
    release_extension = pw.DoubleField(null=True)
    fielder_2 = pw.ForeignKeyField(_Player, backref='as_fielder_2')
    fielder_3 = pw.ForeignKeyField(_Player, backref='as_fielder_3')
    fielder_4 = pw.ForeignKeyField(_Player, backref='as_fielder_4')
    fielder_5 = pw.ForeignKeyField(_Player, backref='as_fielder_5')
    fielder_6 = pw.ForeignKeyField(_Player, backref='as_fielder_6')
    fielder_7 = pw.ForeignKeyField(_Player, backref='as_fielder_7')
    fielder_8 = pw.ForeignKeyField(_Player, backref='as_fielder_8')
    fielder_9 = pw.ForeignKeyField(_Player, backref='as_fielder_9')
    release_pos_y = pw.DoubleField(null=True)
    estimated_ba_using_speedangle = pw.DoubleField(null=True)
    estimated_woba_using_speedangle = pw.DoubleField(null=True)
    woba_value = pw.DoubleField(null=True)
    woba_denom = pw.BigIntegerField(null=True)
    babip_value = pw.BigIntegerField(null=True)
    iso_value = pw.BigIntegerField(null=True)
    at_bat_number = pw.BigIntegerField()
    pitch_number = pw.BigIntegerField()
    home_score = pw.BigIntegerField()
    away_score = pw.BigIntegerField()
    bat_score = pw.BigIntegerField()
    fld_score = pw.BigIntegerField()
    post_away_score = pw.BigIntegerField()
    post_home_score = pw.BigIntegerField()
    post_bat_score = pw.BigIntegerField()
    post_fld_score = pw.BigIntegerField()
    if_fielding_alignment = util._enum_to_field(InFieldingAlignment, null=True)
    of_fielding_alignment = util._enum_to_field(OutFieldingAlignment, null=True)
    spin_axis = pw.BigIntegerField(null=True)
    delta_home_win_exp = pw.DoubleField()
    delta_run_exp = pw.DoubleField(null=True)
    bat_speed = pw.DoubleField(null=True)
    swing_length = pw.DoubleField(null=True)
    estimated_slg_using_speedangle = pw.DoubleField(null=True)
    delta_pitcher_run_exp = pw.DoubleField(null=True)
    hyper_speed = pw.DoubleField(null=True)
    home_score_diff = pw.BigIntegerField()
    bat_score_diff = pw.BigIntegerField()
    home_win_exp = pw.DoubleField()
    bat_win_exp = pw.DoubleField()
    age_pit_legacy = pw.BigIntegerField()
    age_bat_legacy = pw.BigIntegerField()
    age_pit = pw.BigIntegerField()
    age_bat = pw.BigIntegerField()
    n_thruorder_pitcher = pw.BigIntegerField()
    n_priorpa_thisgame_player_at_bat = pw.BigIntegerField()
    pitcher_days_since_prev_game = pw.BigIntegerField(null=True)
    batter_days_since_prev_game = pw.BigIntegerField(null=True)
    pitcher_days_until_next_game = pw.BigIntegerField(null=True)
    batter_days_until_next_game = pw.BigIntegerField(null=True)
    api_break_z_with_gravity = pw.DoubleField(null=True)
    api_break_x_arm = pw.DoubleField(null=True)
    api_break_x_batter_in = pw.DoubleField(null=True)
    arm_angle = pw.DoubleField(null=True)
    attack_angle = pw.DoubleField(null=True)
    attack_direction = pw.DoubleField(null=True)
    swing_path_tilt = pw.DoubleField(null=True)
    intercept_ball_minus_batter_pos_x_inches = pw.DoubleField(null=True)
    intercept_ball_minus_batter_pos_y_inches = pw.DoubleField(null=True)

    class Meta:
        table_name = 'pitch'


def pitch_model(db: pw.SqliteDatabase) -> ty.Type[_Pitch]:
    class Pitch(_Pitch):
        class Meta:  # type: ignore
            table_name = 'pitch'
            database = db

    return Pitch
