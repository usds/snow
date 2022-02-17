import pandas as pd
import datetime
import os
import pathlib
import re


def publish_report_files(df: pd.DataFrame, report_name: str, reports_directory: str):
    report_filename = re.sub(r'[^A-Za-z0-9 ]+', '', report_name).lower().replace(" ", "-")
    report_directory = os.path.join(
        reports_directory, "generated", report_filename, datetime.datetime.now().isoformat())
    pathlib.Path(report_directory).mkdir(parents=True, exist_ok=True)

    report_markdown_filename = report_filename + ".md"
    report_markdown = f"# [{report_name}](../../../docs/{report_markdown_filename})\n\n"
    report_markdown += "\n"
    report_markdown += f"{df.to_markdown(index=False)}"
    with open(os.path.join(report_directory, report_markdown_filename), "w+") as f:
        f.write(report_markdown)

    report_excel_filename = report_filename + ".xlsx"
    df.to_excel(os.path.join(report_directory, report_excel_filename), index=False)

    report_csv_filename = report_filename + ".csv"
    df.to_csv(os.path.join(report_directory, report_csv_filename), index=False)
