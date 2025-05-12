import re
from typing import Callable, Literal
from datatrove.pipeline.readers.base import BaseDiskReader
from datatrove.io import DataFileLike, DataFolderLike
import json
from datatrove.utils.logging import logger
import math

def normalize_wikisection_headings(text: str) -> str:
    """
    Wikitext の「==Heading==」「===Subheading===」など
    等号で囲まれた見出し行を中身だけに置き換え。
    内側の空白がなくても OK。
    """
    # = が2個以上、任意のテキスト（=以外）1文字以上、= が同数以上、行全体
    pattern = re.compile(r'(?m)^\s*={2,}([^=]+?)={2,}\s*$')
    return pattern.sub(lambda m: m.group(1).strip(), text)

def extract_until_two_non_period_lines(text: str) -> str:
    """
    空白行やピリオドで終わらない行をバッファにため込み、
    ピリオド付き行が来たらバッファごと出力。
    ただしピリオドで終わらない行が2行連続したらそこで中断し、バッファは破棄。
    """
    lines = text.splitlines()
    result = ""
    buffer = ""
    non_period_streak = 0

    for line in lines:
        stripped = line.strip()

        # ピリオド付き行なら、まずバッファをフラッシュしてから出力
        if stripped.endswith('.'):
            result += buffer            # これまでの空白／非ピリオド行を出力
            buffer = ""                 # バッファをクリア
            result += line + "\n"       # 今回のピリオド行を出力
            non_period_streak = 0       # 連続カウントをリセット

        else:
            # 空白行・ピリオド無し行はバッファにため込む
            buffer += line + "\n"

            # 実際のテキスト行（空白行以外）が非ピリオドなら連続カウント
            if stripped:
                non_period_streak += 1
                if non_period_streak >= 2:
                    # 2行連続非ピリオド → バッファ破棄＆中断
                    break

    return result.rstrip("\n")


def remove_after_references(text: str) -> str:
    """
    'References', 'External links', 'See also', 'Category:', 'Notes' のいずれかが
    本文中に出てきたら、そのキーワードを含めて以降を削除する。
    ただし、最初の行（タイトル行）に現れた場合は無視する。
    大文字小文字は区別する。
    """
    # 対象キーワードをパイプでつないだ正規表現パターン
    pattern = r'References|External links|See also|Category:|Notes'

    # 最初の行の終了位置を取得
    first_line_end = text.find('\n')
    if first_line_end == -1:
        first_line_end = len(text)

    # キーワードのすべての出現位置を探し、最初の行以降かつ最も先に出たものを見つける
    earliest_valid_match_pos = None
    for match in re.finditer(pattern, text):
        pos = match.start()
        if pos > first_line_end:
            if earliest_valid_match_pos is None or pos < earliest_valid_match_pos:
                earliest_valid_match_pos = pos

    # 最も先に出現した位置があれば、そこから先（キーワード自身を含む）を削除
    if earliest_valid_match_pos is not None:
        return text[:earliest_valid_match_pos].rstrip()

    # マッチがなければ元のテキストを返す
    return text

def contains_western_year(text: str) -> bool:
    """
    英文テキスト中に「ちょうど3桁または4桁の数字の並び」が含まれるかを返す。
    - 末尾や先頭が文字・空白でもマッチ（例: 'in 1990s' → マッチ）
    - 長い数字列の一部にはマッチしない（例: '12345' → マッチしない）
    """
    # (?<!\d) 直前が数字でない／\d{3,4} ちょうど3～4桁／(?!\d) 直後が数字でない
    return bool(re.search(r'(?<!\d)\d{3,4}(?!\d)', text))

class LenientJsonlReader(BaseDiskReader):
    """JsonlReader を継承し、orjson→json.loads に差し替えたもの"""

    def __init__(
        self,
        data_folder: DataFolderLike,
        paths_file: DataFileLike | None = None,
        compression: Literal["infer", "gzip", "zstd"] | None = "infer",
        limit: int = -1,
        skip: int = 0,
        file_progress: bool = False,
        doc_progress: bool = False,
        adapter: Callable = None,
        text_key: str = "text",
        id_key: str = "id",
        default_metadata: dict = None,
        recursive: bool = True,
        glob_pattern: str | None = None,
        shuffle_files: bool = False,
    ):
        super().__init__(
            data_folder,
            paths_file,
            limit,
            skip,
            file_progress,
            doc_progress,
            adapter,
            text_key,
            id_key,
            default_metadata,
            recursive,
            glob_pattern,
            shuffle_files,
        )
        self.compression = compression

    def read_file(self, filepath: str):
        with self.data_folder.open(filepath, "r", compression=self.compression) as f:
            try:
                for li, line in enumerate(f):
                    with self.track_time():
                        try:
                            data = json.loads(line)
                            document = self.get_document_from_dict(data, filepath, li)
                            if not document:
                                continue
                        except Exception as e:
                            logger.warning(f"Error when reading `{filepath}` line {li}: {e}")
                            continue
                    yield document
            except UnicodeDecodeError as e:
                logger.warning(f"File `{filepath}` may be corrupted: {e}")
