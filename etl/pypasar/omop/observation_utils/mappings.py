import pandas as pd

from .config import SOURCE_TABLE_COL_NAME, OBSERVATION_ID_MAPPING, OBSERVATION_DATE_MAPPING, VALUE_AS_STRING_MAPPING
from .util import mapping_wrapper


@mapping_wrapper
def map_observation_id(df: pd.DataFrame) -> pd.DataFrame:
    # Sort and reset index to set dataframe index as running index
    df = df.sort_values(OBSERVATION_ID_MAPPING["pasar"], ascending=[
                        False, False]).reset_index(drop=True)

    # reset_index again to get id as a column
    df = df.reset_index(names=OBSERVATION_ID_MAPPING["omop"])

    # TODO: Seems to have duplicates with OBSERVATION_ID_MAPPING composite, e.g [id, session_startdate] composite is not unique
    # Sanity check for unique OBSERVATION_ID_MAPPING composite
    # df = df[OBSERVATION_ID_MAPPING["pasar"]
    #         ].value_counts().reset_index(name='count')

    return df[[OBSERVATION_ID_MAPPING["omop"]]]


@mapping_wrapper
def map_observation_date(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={OBSERVATION_DATE_MAPPING["pasar"]: OBSERVATION_DATE_MAPPING["omop"]})
    return df[[OBSERVATION_DATE_MAPPING["omop"]]]


@mapping_wrapper
def map_value_as_string(df: pd.DataFrame) -> pd.DataFrame:
    for table_source_name, columns in VALUE_AS_STRING_MAPPING["pasar"]:
        # Concat all text based on table and column config in value_as_string_config
        df.loc[df[SOURCE_TABLE_COL_NAME] == table_source_name, VALUE_AS_STRING_MAPPING["omop"]] = df[columns].apply(
            lambda x: ','.join(x.astype(str)),
            axis=1
        )
    return df[[VALUE_AS_STRING_MAPPING["omop"]]]
