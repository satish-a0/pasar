import pandas as pd

from .config import SOURCE_TABLE_COL_NAME, ObservationMappingConfig, CHUNK_SIZE
from .util import mapping_wrapper


class ObservationMapping():

    @mapping_wrapper
    def map_observation_id(self, df: pd.DataFrame, rowsMapped: int) -> pd.DataFrame:
        observation_id_mapping = ObservationMappingConfig.observation_id_mapping
        # Sort and reset index to set dataframe index as running index
        df = df.sort_values(observation_id_mapping["pasar"], ascending=[
                            False, False]).reset_index(drop=True)

        # Add one to index so that id starts from 1 instead of 0
        df.index += 1 + rowsMapped
        # reset_index again to get id as a column
        df = df.reset_index(names=observation_id_mapping["omop"])

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
        # TODO: Update logic
        visit_occurrence_id_mapping = ObservationMappingConfig.visit_occurrence_id_mapping

        df[visit_occurrence_id_mapping["omop"]
           ] = df[visit_occurrence_id_mapping["pasar"]]
        return df[[visit_occurrence_id_mapping["omop"]]]

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

    @mapping_wrapper
    def map_eav(self, df: pd.DataFrame, source_table: str) -> pd.DataFrame:
        '''Maps
        observation_concept_id
        observation_source_value
        value_source_value
        value_as_number
        value_as_string
        '''

        # TODO: add observation_concept_id mapping
        value_as_string_mapping = ObservationMappingConfig.value_as_string_mapping
        observation_source_value_mapping = ObservationMappingConfig.observation_source_value_mapping
        value_source_value_mapping = ObservationMappingConfig.value_source_value_mapping
        value_as_number_mapping = ObservationMappingConfig.value_as_number_mapping

        mapped_df = pd.DataFrame()
        vas_table_mapping = value_as_string_mapping["pasar"].get(
            source_table, [])
        osb_table_mapping = observation_source_value_mapping["pasar"].get(
            source_table, [])
        vsv_table_mapping = value_source_value_mapping["pasar"].get(
            source_table, [])
        van_table_mapping = value_as_number_mapping["pasar"].get(
            source_table, [])

        # Get list of all possible EAV columns
        eav_columns = list(set(
            vas_table_mapping + osb_table_mapping + vsv_table_mapping + van_table_mapping))

        for eav_column in eav_columns:
            temp_df = df.copy()

            # TODO: add observation_concept_id mapping logic
            temp_df["observation_concept_id"] = 1

            # map value_as_string
            if eav_column in vas_table_mapping:
                temp_df[value_as_string_mapping["omop"]
                        ] = df[eav_column].astype(str)
            else:
                temp_df[value_as_string_mapping["omop"]] = None

            # map observation_source_value
            if eav_column in osb_table_mapping:
                temp_df[observation_source_value_mapping["omop"]
                        ] = eav_column
            else:
                temp_df[observation_source_value_mapping["omop"]] = None

            # map value_source_value
            if eav_column in vsv_table_mapping:
                temp_df[value_source_value_mapping["omop"]
                        ] = df[eav_column].astype(str)
            else:
                temp_df[value_source_value_mapping["omop"]] = None

            # map value_as_number
            if eav_column in van_table_mapping:
                temp_df[value_as_number_mapping["omop"]] = df[eav_column]
            else:
                temp_df[value_as_number_mapping["omop"]] = None

            # Remove eav_column from dataframe
            # temp_df = temp_df.drop(labels=[eav_column], axis=1)
            # df = df.drop(labels=[eav_column], axis=1)

            mapped_df = pd.concat([mapped_df, temp_df], ignore_index=True)

        return mapped_df
