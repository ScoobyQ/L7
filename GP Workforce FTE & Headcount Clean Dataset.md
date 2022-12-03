#### Imports

```python
import pandas as pd
import re

```

#### Globals & helpers

```python
IN_FILE = "GPW Bulletin Tables - October 2022.xlsx" # file to clean should be on folder path

FOLDER_PATH = "<path_to_folder>/" # should end with path separator e.g. /

OUT_FILE = 'gp_wf.csv'

AGE_GROUPS = [
    "Under 25",
    "25-29",
    "Under 30",
    "30-34",
    "35-39",
    "40-44",
    "45-49",
    "50-54",
    "55-59",
    "60-64",
    "65 and over",
    "Unknown"
]
NAMES = {"2a": "fte", "2b": "headcount"}


def get_clean_dataset(sheet_data: pd.DataFrame, name: str) -> pd.DataFrame:

    start_index = (
        sheet_data.index[
            sheet_data.iloc[:, 0].str.contains("All GPs", na=False)
        ].tolist()[0]
        - 1
    )

    end_index = (
        sheet_data.iloc[start_index:, -2]
        .index[~ sheet_data.iloc[start_index:, -2].isnull()]
        .tolist()[-1]
        + 1
    )

    df = sheet_data.iloc[start_index:end_index, :-1]

    header = [
        "01 " + re.sub(r"(\w\s\d{4})(\s.*)", r"\1", str(i).replace("\n", " "))
        if not pd.isna(i)
        else "staff"
        for i in df.iloc[0].values
    ]

    df = df.iloc[1:]
    df.columns = header
    df = df.melt(id_vars="staff", var_name="esd", value_name=NAMES[name])
    df = df[~df.staff.str.contains("estimate")]

    df["staff"] = df["staff"].apply(
        lambda x: re.sub(r"^([A-Za-z\s\(\)]+)((?<!Under\s)[\d,]+)$", r"\1", x).strip()
    )

    staff_groups = df[~df.staff.isin(AGE_GROUPS)].staff.unique()

    df["staff_group"] = [i if i in staff_groups else None for i in df.staff]
    df["staff_group"].ffill(inplace=True)

    df["age_group"] = [i if i in AGE_GROUPS else "All" for i in df.staff]

    df["esd"] = pd.to_datetime(df["esd"])
    df.drop("staff", inplace=True, axis=1)

    return df

```

#### Generate clean dataset

```python
data = pd.read_excel(f'{FOLDER_PATH}{IN_FILE}', sheet_name = list(NAMES.keys()))
d1, d2, *_ = [get_clean_dataset(v, k) for k,v in data.items()]
clean_dataset = d1.merge(d2, on = ['esd', 'staff_group', 'age_group'], how='inner')
clean_dataset = clean_dataset[['esd', 'staff_group', 'age_group'] + list(NAMES.values())]
```

#### Exports

```python
suffix = pd.Period(clean_dataset.esd.max(), freq='M').end_time.date().strftime('%Y%m%d_')
clean_dataset.to_csv(f'{FOLDER_PATH}{suffix}{OUT_FILE}', encoding='utf-8-sig', index=False)
```
