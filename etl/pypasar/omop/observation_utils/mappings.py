import pandas as pd

from .config import SOURCE_TABLE_COL_NAME, ObservationMappingConfig
from .util import mapping_wrapper


class ObservationMapping():

    def __concatenate_multiple_columns_into_one(self, df: pd.DataFrame, observation_mapping: any) -> pd.DataFrame:
        for table_source_name, columns in observation_mapping["pasar"]:
            # Concat all text based on table and column config in value_as_string_config
            # TODO: Look into optimizing function
            df.loc[df[SOURCE_TABLE_COL_NAME] == table_source_name, observation_mapping["omop"]] = df[columns].apply(
                lambda x: ','.join(x.astype(str)),
                axis=1
            )

        # TODO: TO REMOVE: TEMPOARILY ADD SO THAT INGESTION CAN WORK DUE TO VARCHAR(50) CONSTRAINT
        df[observation_mapping["omop"]
           ] = df[observation_mapping["omop"]].str.slice(0, 50)

        return df[[observation_mapping["omop"]]]

    @mapping_wrapper
    def map_observation_id(self, df: pd.DataFrame) -> pd.DataFrame:
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
    def map_person_id(self, df: pd.DataFrame, omop_person_df: pd.DataFrame) -> pd.DataFrame:
        '''Maps pasar anon_case_no to omop person.person_source_value'''
        person_id_mapping = ObservationMappingConfig.person_id_mapping

        # Left join on df
        df = df.merge(omop_person_df,
                      left_on=person_id_mapping["pasar"], right_on='person_source_value', how='left')
        return df[[person_id_mapping["omop"]]]

    @mapping_wrapper
    def map_observation_date(self, df: pd.DataFrame) -> pd.DataFrame:
        observation_date_mapping = ObservationMappingConfig.observation_date_mapping
        df = df.rename(
            columns={observation_date_mapping["pasar"]: observation_date_mapping["omop"]})
        return df[[observation_date_mapping["omop"]]]

    @mapping_wrapper
    def map_observation_type_concept_id(self, df: pd.DataFrame) -> pd.DataFrame:
        observation_type_concept_id_mapping = ObservationMappingConfig.observation_type_concept_id_mapping

        # Just set observation_type_concept_id as hardcoded value
        df[observation_type_concept_id_mapping["omop"]] = 32879
        return df[[observation_type_concept_id_mapping["omop"]]]

    @mapping_wrapper
    def map_visit_occurrence_id(self, df: pd.DataFrame) -> pd.DataFrame:
        visit_occurrence_id_mapping = ObservationMappingConfig.visit_occurrence_id_mapping

        df[visit_occurrence_id_mapping["omop"]
           ] = df[visit_occurrence_id_mapping["pasar"]]
        return df[[visit_occurrence_id_mapping["omop"]]]

    @mapping_wrapper
    def map_value_as_string(self, df: pd.DataFrame) -> pd.DataFrame:
        value_as_string_mapping = ObservationMappingConfig.value_as_string_mapping
        return self.__concatenate_multiple_columns_into_one(df, value_as_string_mapping)

    @mapping_wrapper
    def map_observation_source_value(self, df: pd.DataFrame) -> pd.DataFrame:
        observation_source_value_mapping = ObservationMappingConfig.observation_source_value_mapping
        return self.__concatenate_multiple_columns_into_one(df, observation_source_value_mapping)

    @mapping_wrapper
    def map_value_source_value(self, df: pd.DataFrame) -> pd.DataFrame:
        value_source_value_mapping = ObservationMappingConfig.value_source_value_mapping
        return self.__concatenate_multiple_columns_into_one(df, value_source_value_mapping)

    @mapping_wrapper
    def map_value_as_number(self, df: pd.DataFrame) -> pd.DataFrame:
        value_as_number_mapping = ObservationMappingConfig.value_as_number_mapping

        for table_source_name, column in value_as_number_mapping["pasar"]:
            df.loc[df[SOURCE_TABLE_COL_NAME] == table_source_name,
                   value_as_number_mapping["omop"]] = df[column]
        return df[[value_as_number_mapping["omop"]]]

    @mapping_wrapper
    def map_value_as_concept_id(self, df: pd.DataFrame, allergy_concepts_df: pd.DataFrame) -> pd.DataFrame:
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
