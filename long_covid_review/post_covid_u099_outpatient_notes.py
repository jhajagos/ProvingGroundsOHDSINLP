import sqlalchemy as sa
import rubrix as rb
import json
import httpx

def main(config):

    rb.init(api_url=config["rubrix_uri"], api_key=config["rubrix_api_key"])
    rb.set_workspace(config["rubrix_workspace"])

    connection_uri = config["connection_uri"]
    engine = sa.create_engine(connection_uri)
    result_list = []

    with engine.connect() as connection:

        # Long Covid Patients; notes 1 month after DX or test, Outpatient physician notes
        query = """
        select pn.*, e.service_delivery_location from sbm_covid19_documents.physician_notes pn
    join sbm_covid19_hi.PH_F_Encounter e on pn.encounter_id = e.encounter_number
    where document_id in (
        select distinct document_id from sbm_covid19_documents.physician_notes p
            join sbm_covid19_analytics_build.pui_covid_result_overview pcro
                on p.patient_id = pcro.mrn
            join sbm_covid19_hi_cdm_build.map2_condition_occurrence co on pcro.person_id = co.person_id
            and co.condition_source_concept_code = 'U09.9'
        where p.document_datetime >= pcro.positive_datetime + interval '1 month' and pcro.positive_datetime is not null)
    and document_type not in ('Ambulatory Patient Summary')
    and e.classification_display = 'Outpatient'
        """

        cursor = connection.execute(query)
        report_list = []
        for row in cursor:

            result_list += [row["file_text"]]
            meta_data_dict = {}
            columns = ["patient_id", "mapped_document_type", "document_type", "service_delivery_location", "patient_id",
                       "encounter_id"]
            for column in columns:
                meta_data_dict[column] = row[column]

            rb_obj = rb.TextClassificationRecord(inputs={"text": row["file_text"]}, metadata=meta_data_dict, id=row["document_id"])
            report_list += [rb_obj]

        reports_to_commit = []
        for i in range(len(report_list)):

            reports_to_commit += [report_list[i]]
            if i > 0 and i % 50 == 0:
                rb.log(records=reports_to_commit, name="long_covid_with_metadata")
                reports_to_commit = []
            else:
                pass

        if len(reports_to_commit):
            rb.log(records=reports_to_commit, name="long_covid_with_metadata")


if __name__ == "__main__":

    with open("./config.json") as f:
        config = json.load(f)

    main(config)