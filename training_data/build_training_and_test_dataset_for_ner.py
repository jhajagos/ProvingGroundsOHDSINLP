import sqlalchemy as sa
import spacy
import json
import re
from spacy.tokens import DocBin
import pathlib
import random
import math

"""
Builds two data set for training and evaluation for NER training in SPACY

We query the OHDSI tables (note_nlp) to get annotations

We align the lexical variant using SPACY nlp

"""


def main(connection_uri, schema_name, output_path, max_size, train_split):

    engine = sa.create_engine(connection_uri)
    with engine.connect() as connection:
        meta_data = sa.MetaData(connection, schema=schema_name)
        meta_data.reflect()

        note_nlp_obj = meta_data.tables[schema_name + "." + "note_nlp"]
        query_obj = note_nlp_obj.select().where(note_nlp_obj.c.note_nlp_concept_id != 4266367).order_by(note_nlp_obj.c.note_id)  # Exclude FLU as there are many false positives

        cursor = connection.execute(query_obj)
        snippet_dict = {}

        re_date = re.compile("[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2} EST")

        i = 0
        for row in cursor:

            snippet = row["snippet"].strip()
            re_snippet_obj = re_date.search(snippet)

            if re_snippet_obj is not None:
                snippet = snippet[re_snippet_obj.end():]
                snippet = snippet.strip()

            lexical_variant = row["lexical_variant"]
            note_nlp_concept_id = row["note_nlp_concept_id"]
            note_id = row["note_id"]
            note_nlp_id = row["note_nlp_id"]
            term_modifiers = row["term_modifiers"]
            modifier_list = term_modifiers.split(",")

            if lexical_variant in snippet:
                start_offset = snippet.index(lexical_variant)
                end_offset = start_offset + len(lexical_variant)

                match_dict = {
                             "lexical_variant": lexical_variant, "note_id": note_id,
                              "note_nlp_id": note_nlp_id,
                              "term_modifiers": modifier_list,
                              "note_nlp_concept_id": note_nlp_concept_id,
                              "offsets": (start_offset, end_offset)
                             }

                if snippet in snippet_dict:
                    snippet_dict[snippet] += [match_dict]
                else:
                    snippet_dict[snippet] = [match_dict]

            if i == max_size:
               break

            i += 1

        # Build annotations
        nlp = spacy.load("en_core_web_sm")
        doc_list = []
        for snippet in snippet_dict:
            doc = nlp(snippet)
            variants_found = []
            spans = []
            for variant_dict in snippet_dict[snippet]:
                variant = variant_dict["lexical_variant"]
                start_position, end_position = variant_dict["offsets"]
                term_modifiers = variant_dict["term_modifiers"]

                annotation_label = term_modifiers[0].split("=")[1]

                if variant not in variants_found and annotation_label is not None:
                    variants_found += [variant]
                    spans += [doc.char_span(start_position, end_position, label=annotation_label, alignment_mode="expand")]

            doc.set_ents(spans)
            doc_list += [doc]

        # Shuffle list to randomize
        random.shuffle(doc_list)
        number_of_documents = len(doc_list)

        test_size = int(math.floor(number_of_documents * train_split))
        training_size = number_of_documents - test_size

        p_output_path = pathlib.Path(output_path)
        training_corpora_path = p_output_path / "sbm_ohdsi_train_covid_annotated.spacy"

        train_doc_bin_obj = DocBin(docs=doc_list[0:training_size])
        train_doc_bin_obj.to_disk(training_corpora_path)

        test_doc_bin_obj = DocBin(docs=doc_list[training_size:])
        testing_corpora_path = p_output_path / "sbm_ohdsi_test_covid_annotated.spacy"
        test_doc_bin_obj.to_disk(testing_corpora_path)




if __name__ == "__main__":
    with open("./config.json") as f:
        config = json.load(f)

    main(config["connection_uri"], config["schema"], config["data_directory"], 20000, 0.3)