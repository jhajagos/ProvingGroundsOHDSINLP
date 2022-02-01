import pandas as pd
import spacy
import json
import pathlib
import numpy as np


def calculate_false_positives(row, column_gs, column_ml):

    value_gs = row[column_gs]
    value_ml = row[column_ml]

    if value_gs == "No":
        if value_ml == "Yes":
            return 1
        else:
            return 0

    return np.nan


def calculate_false_negatives(row, column_gs, column_ml):

    value_gs = row[column_gs]
    value_ml = row[column_ml]

    if value_gs == "Yes":
        if value_ml is np.nan:
            return 1
        else:
            return 0

    return np.nan


def calculate_true_positives(row, column_gs, column_ml):
    value_gs = row[column_gs]
    value_ml = row[column_ml]

    if value_gs == "Yes":
        if value_ml == "Yes":
            return 1
        else:
            return 0

    return np.nan


def calculate_true_negatives(row, column_gs, column_ml):
    value_gs = row[column_gs]
    value_ml = row[column_ml]

    if value_gs == "No":
        if value_ml is np.nan:
            return 1
        else:
            return 0

    return np.nan


def main(config):
    data_directory = config["data_directory"]

    p_data_directory = pathlib.Path(data_directory)

    merged_df = pd.read_csv(p_data_directory / "merged_nlp_with_manual_chart_review.csv")

    for pair in pairs_for_comparison:
        chart_review_column, nlp_column = pair

        print("")
        print(f"Comparing: '{chart_review_column}' to '{nlp_column}'")

        def lambda_fp(row):
            return calculate_false_positives(row, chart_review_column, nlp_column)

        merged_df[nlp_column + "|fp"] = merged_df.apply(lambda_fp, axis=1)

        def lambda_fn(row):
            return calculate_false_negatives(row, chart_review_column, nlp_column)

        merged_df[nlp_column + "|fn"] = merged_df.apply(lambda_fn, axis=1)

        def lambda_tp(row):
            return calculate_true_positives(row, chart_review_column, nlp_column)

        merged_df[nlp_column + "|tp"] = merged_df.apply(lambda_tp, axis=1)

        def lambda_tn(row):
            return calculate_true_negatives(row, chart_review_column, nlp_column)

        merged_df[nlp_column + "|tn"] = merged_df.apply(lambda_tn, axis=1)

        compare_sum_series = merged_df[[nlp_column + "|fn", nlp_column + "|fp", nlp_column + "|tp", nlp_column + "|tn"]].sum()

        print(compare_sum_series)

        fn, fp, tp, tn = compare_sum_series.values.tolist()

        print(f"Sensitivity: {tp / (tp + fn)}")
        print(f"Specificity: {tn / (tn + fp)}")
        print(f"PPV: {tp / (tp + fp)}")
        print(f"Frequency: {(tp + fn) / (tp + fn + tn + fp)}")

        file_name_to_export = p_data_directory / "merged_nlp_with_manual_chart_review_with_comp.csv"

        print("")
        print(f"Writing: '{file_name_to_export}'")
        merged_df.to_csv(file_name_to_export, index=False)


if __name__ == "__main__":

    with open("config.json") as f:
        config = json.load(f)

    pairs_for_comparison = [("abdominal_pain_v", "note_nlp_concept_name=Abdominal pain"),
                            ("diarrhea_v", "note_nlp_concept_name=Diarrhea"),
                            ("dyspnea_admission_v", "note_nlp_concept_name=Dyspnea"),
                            ("cough_v", "note_nlp_concept_name=Dry cough"),
                            ("fever_v", "note_nlp_concept_name=Fever")]

    main(config)