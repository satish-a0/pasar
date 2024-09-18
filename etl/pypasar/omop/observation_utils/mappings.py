import pandas as pd

from .config import SOURCE_TABLE_COL_NAME, ObservationMappingConfig
from .util import mapping_wrapper


class ObservationMappings():
    @mapping_wrapper
    def map_observation_id(df: pd.DataFrame) -> pd.DataFrame:
        observation_id_mapping = ObservationMappingConfig.observation_id_mapping
        # Sort and reset index to set dataframe index as running index
        df = df.sort_values(observation_id_mapping["pasar"], ascending=[
                            False, False]).reset_index(drop=True)

        # reset_index again to get id as a column
        df = df.reset_index(names=observation_id_mapping["omop"])

        # TODO: Seems to have duplicates with observation_id_mapping composite, e.g [id, session_startdate] composite is not unique
        # Sanity check for unique observation_id_mapping composite
        # test_df = df[observation_id_mapping["pasar"]
        #              ].value_counts().reset_index(name='count')
        # print(test_df)
        # test = df.loc[(df["id"] == 4552) & (
        #     df["session_startdate"].astype('str') == "2013-03-07")]
        # print(test[["id", "session_startdate", "source_table"]])

        return df[[observation_id_mapping["omop"]]]

    @mapping_wrapper
    def map_observation_date(df: pd.DataFrame) -> pd.DataFrame:
        observation_date_mapping = ObservationMappingConfig.observation_date_mapping
        df = df.rename(
            columns={observation_date_mapping["pasar"]: observation_date_mapping["omop"]})
        return df[[observation_date_mapping["omop"]]]

    @mapping_wrapper
    def map_observation_type_concept_id(df: pd.DataFrame) -> pd.DataFrame:
        observation_type_concept_id_mapping = ObservationMappingConfig.observation_type_concept_id_mapping

        # Just set observation_type_concept_id as hardcoded value
        df[observation_type_concept_id_mapping["omop"]] = 32879
        return df[[observation_type_concept_id_mapping["omop"]]]

    @mapping_wrapper
    def map_visit_occurrence_id(df: pd.DataFrame) -> pd.DataFrame:
        visit_occurrence_id_mapping = ObservationMappingConfig.visit_occurrence_id_mapping

        df[visit_occurrence_id_mapping["omop"]
           ] = df[visit_occurrence_id_mapping["pasar"]]
        return df[[visit_occurrence_id_mapping["omop"]]]

    # Shared between value_as_string and observation_source_value
    @mapping_wrapper
    def concatenate_multiple_columns_into_one(df: pd.DataFrame, observation_mapping: any) -> pd.DataFrame:
        for table_source_name, columns in observation_mapping["pasar"]:
            # Concat all text based on table and column config in value_as_string_config
            df.loc[df[SOURCE_TABLE_COL_NAME] == table_source_name, observation_mapping["omop"]] = df[columns].apply(
                lambda x: ','.join(x.astype(str)),
                axis=1
            )
        return df[[observation_mapping["omop"]]]

    @mapping_wrapper
    def map_value_as_number(df: pd.DataFrame) -> pd.DataFrame:
        value_as_number_mapping = ObservationMappingConfig.value_as_number_mapping

        for table_source_name, column in value_as_number_mapping["pasar"]:
            df.loc[df[SOURCE_TABLE_COL_NAME] == table_source_name,
                   value_as_number_mapping["omop"]] = df[column]
        return df[[value_as_number_mapping["omop"]]]

    @mapping_wrapper
    def map_value_as_concept_id(df: pd.DataFrame, allergy_concepts_df: pd.DataFrame) -> pd.DataFrame:
        value_as_concept_id_mapping = ObservationMappingConfig.value_as_concept_id_mapping

        # Strip "Allergy to " prefix from concept_name
        allergy_concepts_df['allergy_concept'] = allergy_concepts_df['concept_name'].str.removeprefix(
            "Allergy to ")

        # Lower case both columns used for joining
        allergy_concepts_df['allergy_concept'] = allergy_concepts_df['allergy_concept'].str.lower(
        )
        df[value_as_concept_id_mapping["pasar"]
           ] = df[value_as_concept_id_mapping["pasar"]].str.lower()

        # Left join on df
        df = df.merge(allergy_concepts_df,
                      left_on=value_as_concept_id_mapping["pasar"], right_on='allergy_concept', how='left')

        # Rename concept_id column as value_as_concept_id and set dtype to int
        df = df.rename(
            columns={"concept_id": value_as_concept_id_mapping["omop"]})
        df[value_as_concept_id_mapping["omop"]
           ] = df[value_as_concept_id_mapping["omop"]].astype(pd.Int64Dtype())

        return df[[value_as_concept_id_mapping["omop"]]]
