import pandas as pd
import spacy
import json
import pathlib


def main(config, concept_names):

    p_data_directory = pathlib.Path(config["data_directory"])

    matched_df = pd.read_csv(p_data_directory / "merged_nlp_with_manual_chart_review_with_comp.csv")
    cross_walk_df = pd.read_csv(p_data_directory / "matched_and_filtered_notes_with_matched_concepts.csv")
    meta_data_df = pd.read_csv(p_data_directory / "meta_data.csv")

    spacy_annotated_file = p_data_directory / "ohnlp_full_set_label_positive_concepts.spacy"

    nlp = spacy.load("en_core_web_sm")
    doc_bin = spacy.tokens.DocBin()
    doc_bin.from_disk(spacy_annotated_file)
    iter_docs = doc_bin.get_docs(vocab=nlp.vocab)

    doc_list = list(iter_docs)

    doc_position_with_sha1_list_dict = [{"position": i, "sha1": doc_list[i].user_data["sha1"], "text": doc_list[i].text}
                                        for i in range(len(doc_list))]
    doc_position_with_sha1_df = pd.DataFrame(doc_position_with_sha1_list_dict)

    i = 0
    for concept_name in concept_names:

        print(f"Processing '{concept_name}'")

        false_positives_df = matched_df[matched_df[f"note_nlp_concept_name={concept_name}|fp"] == 1]

        fp_with_crosswalk_df = false_positives_df[["encounter_number"]].merge(
            cross_walk_df[cross_walk_df.note_nlp_concept_name == concept_name], on="encounter_number")

        fp_with_crosswalk_sha1_df = fp_with_crosswalk_df.merge(meta_data_df, on="note_nlp_id")

        fp_with_crosswalk_sha1_pos_df = fp_with_crosswalk_sha1_df.merge(doc_position_with_sha1_df, on="sha1")

        fp_with_crosswalk_sha1_pos_df = fp_with_crosswalk_sha1_pos_df.sort_values(["encounter_number", "note_class_concept_id", "note_id_x", "sha1"])

        if i == 0:
            combined_fp_df = fp_with_crosswalk_sha1_pos_df.copy()
        else:
            combined_fp_df = pd.concat([combined_fp_df, fp_with_crosswalk_sha1_pos_df])

        i += 1

    def clean_text_for_excel(text):

        if '-' == str(text)[0]:
            return "'" + str(text) + "'"
        else:
            return str(text)

    combined_fp_df["text"] = combined_fp_df["text"].map(clean_text_for_excel)

    combined_fp_df.to_csv(p_data_directory / "false_positive_examples.csv", index=False)


if __name__ == "__main__":

    with open("config.json") as f:
        config = json.load(f)

    concept_names_to_check = ["Dry cough", "Dyspnea", "Diarrhea", "Abdominal pain"]

    main(config, concept_names_to_check)