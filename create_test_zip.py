import os
import zipfile

# ZIPファイルを作成
with zipfile.ZipFile("data/test_archive.zip", "w") as zipf:
    # CSVファイルをZIPに追加
    zipf.writestr(
        "test_data.csv",
        "date,value\n2024-01-01,1000\n2024-01-02,2000\n2024-01-03,3000\n",
    )
    zipf.writestr("other_file.txt", "This is a text file\n")

print("ZIP file created: data/test_archive.zip")
