from datatrove.executor import LocalPipelineExecutor
from datatrove.pipeline.readers import HuggingFaceDatasetReader
from datatrove.pipeline.writers import JsonlWriter
from datatrove.pipeline.filters import LambdaFilter
from clean import (
    normalize_wikisection_headings,
    extract_until_two_non_period_lines,
    remove_after_references,
    contains_western_year,
)

OUTPUT_FOLDER = "/iwork/takahashi/wiki_processed/pile_test"
#OUTPUT_FILENAME = "pile_processed.jsonl.gz"

import re

def custom_adapter(self, document) -> dict:
    """
    先頭行を必ずタイトルとして取得し、
    その後の連続する空行をスキップして本文とする。
    """
    # 1) 前処理チェーン
    full_text = normalize_wikisection_headings(document.text)
    full_text = remove_after_references(full_text)

    # 2) タイトルと本文を分離
    lines = full_text.splitlines()

    if not lines:
        raise ValueError(f"Document.text が空です\n{document.text=}")

    # 先頭行をタイトルに確定
    title = lines[0].strip()

    # 先頭行の次から本文を探す
    idx = 1
    while idx < len(lines) and not lines[idx].strip():
        idx += 1

    # 本文
    body_text = "\n".join(lines[idx:]).strip()

    # 3) 年号検出・抽出処理
    has_year = contains_western_year(body_text)
    extracted_text = extract_until_two_non_period_lines(body_text)
    extracted_has_year = contains_western_year(extracted_text)

    # 4) 単語数カウント関数
    def count_words(s):
        return len(s.split())

    # 5) 出力用辞書を組み立て
    return {
        "title": title,
        "text": body_text,
        "text_word_count": count_words(body_text),
        "extracted_text_length": len(extracted_text),
        "extracted_text_word_count": count_words(extracted_text),
        "contains_western_year": has_year,
        "extracted_contains_western_year": extracted_has_year,
    }

if __name__ == "__main__":
    pipeline_exec = LocalPipelineExecutor(
        pipeline=[
            HuggingFaceDatasetReader(
                dataset=f"monology/pile-uncopyrighted",
                dataset_options={"data_files": {"test": "test.jsonl.zst"},"split": "test"},
                streaming=True,
                doc_progress=True,
            ),
            LambdaFilter(
                lambda doc: doc.metadata.get("meta", {}).get("pile_set_name") == "Wikipedia (en)",
            ),
            JsonlWriter(
                output_folder=OUTPUT_FOLDER,
                #output_filename=OUTPUT_FILENAME,
                compression="gzip",
                adapter=custom_adapter, 
            ),
        ],
        tasks=8,  # 並列ワーカー数
    )
    pipeline_exec.run()
