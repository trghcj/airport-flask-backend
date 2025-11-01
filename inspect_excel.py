import pandas as pd
file_path = r"C:\Users\suremdra singh\Desktop\Upload Details Position as on 30th June 2025.xlsx"
for sheet in pd.ExcelFile(file_path).sheet_names:
    df = pd.read_excel(file_path, sheet_name=sheet, header=0)
    print(f"Sheet: {sheet}")
    print(df.dtypes)
    print(df.head())
            