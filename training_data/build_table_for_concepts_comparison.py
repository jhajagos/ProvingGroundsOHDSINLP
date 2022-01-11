import json
import sqlalchemy as sa
import pandas as pd
from sklearn.feature_extraction import DictVectorizer
import pathlib
import json
import numpy as np


def main(config):

    connection_uri = config["connection_uri"]
    engine = sa.create_engine(connection_uri)

    p_data_directory = pathlib.Path(config["data_directory"])

    with engine.connect() as connection:

        q1 = """
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

        print(q1)

        note_df = pd.read_sql(q1, connection)

        print(f"Number of rows returned: {len(note_df)}")

        note_df = note_df[note_df["note_class_concept_name"].isin(["Emergency medicine", "Admission evaluation"])]

        print(f"Number of rows after filtering: {len(note_df)}")

        core_df = note_df[["encounter_number", "mrn", "visit_start_datetime", "visit_end_datetime", "visit_concept_name"]].drop_duplicates()
        print(f"Number of visits: {len(core_df)}")

        matched_df = note_df[note_df["note_nlp_concept_name"] != "NC"]
        nc_df = note_df[note_df["note_nlp_concept_name"] == "NC"]

        nc_df = nc_df[["encounter_number", "note_class_concept_name"]].drop_duplicates()

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


        #group_with_no_match_df.to_csv("./output.csv", index=False)

        q2 = """
        select mcr.*
       from sbm_covid19_analytics_build.critical_covid_manual_chart_review mcr
join
(
    select distinct cast(visit_occurrence_id as varchar(36)) as encounter_number
        from sbm_covid19_documents.note n join sbm_covid19_hi_cdm_build.concept c
                on c.concept_id = n.note_class_concept_id
        where c.concept_name in ('Emergency medicine', 'Admission evaluation')
) t on t.encounter_number = mcr.encounter_number
join
(
select v.visit_occurrence_id from sbm_covid19_analytics_build.critical_covid_visits_linked_to_hi v
        join sbm_covid19_analytics_build.checked_visit_index cv on cv.visit_occurrence_id = v.visit_occurrence_id
            where encounter_number is not null and v.covid19_status = 'positive'
) tt on tt.visit_occurrence_id = mcr.visit_occurrence_id
where visit_start_datetime < load_time 
order by visit_start_datetime desc
        """

        print(q2)
        manual_chart_df = pd.read_sql(q2, connection)
        print(f"Number of manual chart reviews: {len(manual_chart_df)}")

        manual_chart_df["encounter_number"] = manual_chart_df["encounter_number"].astype("int64")

        print(f"Number of matched records NLP: {len(group_with_no_match_df)}")
        mr_group_with_no_match_df = group_with_no_match_df.merge(manual_chart_df[["encounter_number"]], on="encounter_number")
        print(f"Number of matched records NLP: {len(mr_group_with_no_match_df)}")

        mr_group_with_match_df = mr_group_with_no_match_df[~mr_group_with_no_match_df.note_nlp_concept_name.isna()]

        grouped_note_class_concept_name_df = mr_group_with_match_df.groupby(["encounter_number"])[["note_nlp_concept_name"]].agg(lambda df: sorted(df.unique().tolist()))
        grouped_note_class_concept_name_df = grouped_note_class_concept_name_df.reset_index()

        grouped_note_class_concept_name_df = grouped_note_class_concept_name_df.sort_values(["encounter_number"])

        json_output_file_name = p_data_directory / "encounter_concept_names.json"
        grouped_note_class_concept_name_df.to_json(json_output_file_name, "records")

        with open(json_output_file_name) as f:
            concept_names_dict = json.load(f)

        dv_obj = DictVectorizer()

        x_concept_d = dv_obj.fit_transform(concept_names_dict)
        x_concept_dense = x_concept_d.todense()
        x_concept_dense = np.array(x_concept_dense, dtype="int64")

        x_concept_df = pd.DataFrame(x_concept_dense)
        x_concept_df.columns = dv_obj.feature_names_

        concept_names = list(x_concept_df.columns)[1:]
        concept_df = x_concept_df.copy()

        def encode_binary_column(col):
            if col == 1:
                return 'Yes'
            else:
                return np.nan

        for name in concept_names:
            concept_df[name] = concept_df[name].apply(encode_binary_column)

        mr_columns = list(manual_chart_df.columns)
        columns_to_include = [c for c in mr_columns if c[-2:] != "_c"]

        ab_manual_chart_df = manual_chart_df[columns_to_include]
        merged_ab_manual_chart_df = ab_manual_chart_df.merge(concept_df, on="encounter_number", how="left")

        merged_ab_manual_chart_df.to_csv(p_data_directory / "merged_nlp_with_manual_chart_review.csv", index=False)

if __name__ == "__main__":
    with open('./config.json') as f:
        config = json.load(f)

    main(config)