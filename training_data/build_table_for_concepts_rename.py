import json
import sqlalchemy as sa
import pandas as pd


def main(config):

    connection_uri = config["connection_uri"]
    engine = sa.create_engine(connection_uri)

    with engine.connect() as connection:

        q = """
        with selected_positive_visits as (
    select cast(v.encounter_number as bigint) as encounter_number,
           v.visit_start_datetime,
           v.visit_end_datetime,
           v.visit_concept_name
    from sbm_covid19_analytics_build.critical_covid_visits_linked_to_hi v
        join sbm_covid19_analytics_build.checked_visit_index cv on cv.visit_occurrence_id = v.visit_occurrence_id
            where encounter_number is not null and encounter_number ~ '^[0-9]+' and v.covid19_status = 'positive')
 select * from (
                   select distinct sv.*,
                                   n.note_id,
                                   n.person_id                                          as mrn,
                                   c1.concept_name                                      as note_class_concept_name,
                                   note_class_concept_id,
                                   note_nlp_concept_id,
                                   case when note_nlp_concept_name is null then 'NC' else note_nlp_concept_name end as note_nlp_concept_name,
                                   case when x.note_id is not null then 1 end as counter
                   from selected_positive_visits sv
                            join sbm_covid19_documents.note n on n.visit_occurrence_id = sv.encounter_number
                            join sbm_covid19_hi_cdm_build.concept c1
                                 on c1.concept_id = n.note_class_concept_id
                            left outer join
                            (select nl.note_id,
                                    c2.concept_id   as note_nlp_concept_id,
                                    c2.concept_name as note_nlp_concept_name
                             from sbm_covid19_documents.note_nlp nl
                                      join sbm_covid19_hi_cdm_build.concept c2 on c2.concept_id = nl.note_nlp_concept_id
                             where left(term_modifiers, 18) = 'certainty=Positive'
                               and c2.concept_name != 'Influenza') x
                        on x.note_id = n.note_id
               ) t
            order by encounter_number, note_class_concept_name, note_nlp_concept_id
        """

        df = pd.read_sql(q, connection)

        # print(len(df))

        df = df[df["note_class_concept_name"].isin(["Emergency medicine", "Discharge summary"])]

        # print(len(df))

        core_df = df[["encounter_number", "mrn", "visit_start_datetime", "visit_end_datetime", "visit_concept_name"]].drop_duplicates()

        matched_df = df[df["note_nlp_concept_name"] != "NC"]
        nc_df = df[df["note_nlp_concept_name"] == "NC"]

        nc_df = nc_df[["encounter_number", "note_class_concept_name"]].drop_duplicates()
        # print(nc_df)

        grouped_df = matched_df[["encounter_number", "note_class_concept_name", "note_nlp_concept_name", "counter"]].\
            groupby(["encounter_number", "note_class_concept_name", "note_nlp_concept_name"])["counter"].agg("sum")

        grouped_df = grouped_df.reset_index()

        grouped_class_df = grouped_df[["encounter_number", "note_class_concept_name"]].drop_duplicates()
        grouped_class_df["has_concept"] = 1

        joined_nc_group_df = pd.merge(nc_df, grouped_class_df,
                                      left_on=("encounter_number", "note_class_concept_name"),
                                      right_on=("encounter_number", "note_class_concept_name"),
                                      how="left", suffixes=("_nc", "_c"))

        no_match_df = joined_nc_group_df[joined_nc_group_df["has_concept"].isna()]
        no_match_df = no_match_df[["encounter_number", "note_class_concept_name"]]

        group_with_no_match_df = pd.concat([grouped_df, no_match_df])
        group_with_no_match_df = group_with_no_match_df.sort_values(["encounter_number", "note_class_concept_name", "note_nlp_concept_name"])

        group_with_no_match_df.to_csv("./output.csv", index=False)

        #print(grouped_df)



if __name__ == "__main__":
    with open('./config.json') as f:
        config = json.load(f)

    main(config)