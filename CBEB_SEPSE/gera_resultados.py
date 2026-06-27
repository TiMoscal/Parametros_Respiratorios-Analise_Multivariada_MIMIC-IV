import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

# =========================================================
# 1) CAMINHOS
# =========================================================

pacientes = r"/Users/giuliamello/Desktop/PROJ MIMIC/pacientes_sepse_cbeb.csv"

output = r"/Users/giuliamello/Desktop/PROJ MIMIC/resultados_pacientes_sepse_cbeb.csv"

chartevents = r"/Users/giuliamello/Desktop/PROJ MIMIC/mimic-iv-moscal/chartevents.csv.gz"
inputevents = r"/Users/giuliamello/Desktop/PROJ MIMIC/mimic-iv-moscal/inputevents.csv.gz"
labevents = r"/Users/giuliamello/Desktop/PROJ MIMIC/mimic-iv-moscal/labevents.csv.gz"

# =========================================================
# 2) CONFIGURAÇÃO DAS COLUNAS DA TABELA BASE
# =========================================================

COL_HADM_ID = "hadm_id"
COL_DATA_DIA = "data_dia"

# =========================================================
# 3) CONFIGURAÇÃO DAS VARIÁVEIS
# =========================================================

CHARTEVENTS_VARS = [
    {"itemid": 220224, "coluna": "PO2 (Arterial)"},
    {"itemid": 220235, "coluna": "PCO2 (Arterial)"},
    {"itemid": 223830, "coluna": "PH (Arterial)"},
    {"itemid": 223835, "coluna": "FiO2"},
    {"itemid": 220227, "coluna": "SaO2"},
    {"itemid": 220050, "coluna": "ABPs"},
    {"itemid": 220051, "coluna": "ABPd"},
    {"itemid": 220052, "coluna": "ABPm"},
    {"itemid": 220045, "coluna": "HR"},
    {"itemid": 220074, "coluna": "CVP"},
    {"itemid": 225690, "coluna": "Bilirubin"},
    {"itemid": 227457, "coluna": "Platelet Count"},
    {"itemid": 220739, "coluna": "GCS - Eye Opening"},
    {"itemid": 223900, "coluna": "GCS - Verbal Response"},
    {"itemid": 223901, "coluna": "GCS - Motor Response"},
    {"itemid": 223761, "coluna": "Temperature F"},
    {"itemid": 220210, "coluna": "RR"},
]

INPUTEVENTS_VARS = [
    {"itemid": 222315, "coluna": "Vasopressin"},
]

LABEVENTS_VARS = [
    {"itemid": [50813, 52442], "coluna": "Lactate"},
    {"itemid": 50819, "coluna": "PEEP"},
    {"itemid": [50820, 52041], "coluna": "pH_lab"},
    {"itemid": [52042, 50821], "coluna": "pO2_lab"},
]

# =========================================================
# 4) FUNÇÕES AUXILIARES
# =========================================================

def gerar_eventos_agregados(df, group_cols, value_col, itemid_col, nome_procedure):
    df = df.copy()

    agregados_dia = (
        df.groupby(group_cols + [itemid_col])[value_col]
        .agg(
            min_dia="min",
            max_dia="max",
            med_dia="mean"
        )
        .reset_index()
    )

    out = df.merge(
        agregados_dia,
        on=group_cols + [itemid_col],
        how="left"
    )

    out = out.rename(columns={
        COL_HADM_ID: "hadm_id",
        "charttime": "time",
        itemid_col: "id_procedure",
        value_col: "resultado"
    })

    out["procedure_name"] = nome_procedure

    return out[
        [
            "hadm_id",
            "time",
            "id_procedure",
            "procedure_name",
            "resultado",
            "min_dia",
            "max_dia",
            "med_dia"
        ]
    ]


def preparar_base(df_base):
    df_base = df_base.copy()

    df_base[COL_HADM_ID] = pd.to_numeric(df_base[COL_HADM_ID], errors="coerce")
    df_base = df_base.dropna(subset=[COL_HADM_ID])
    df_base[COL_HADM_ID] = df_base[COL_HADM_ID].astype(int)

    return df_base


def filtrar_para_hadm_ids_base(chunk, hadm_ids_base):
    if COL_HADM_ID not in chunk.columns:
        return chunk.iloc[0:0].copy()

    return chunk[chunk[COL_HADM_ID].isin(hadm_ids_base)].copy()


# =========================================================
# 5) LEITURA DA BASE
# =========================================================

print("Lendo base...")

base = pd.read_csv(pacientes)
base = preparar_base(base)

hadm_ids_base = set(base[COL_HADM_ID].dropna().astype(int).unique())

print(f"Linhas da base: {len(base):,}")
print(f"hadm_id únicos: {len(hadm_ids_base):,}")

eventos_final = []

# =========================================================
# 6) PROCESSAR CHARTEVENTS
# =========================================================

char_vars_validas = [v for v in CHARTEVENTS_VARS if v["itemid"] != 0]
char_itemids = sorted(set(v["itemid"] for v in char_vars_validas))

if char_itemids:
    print("\nProcessando chartevents...")

    usecols_char = [COL_HADM_ID, "itemid", "charttime", "valuenum"]
    lista_char = []
    chunksize = 1_000_000

    for i, chunk in enumerate(pd.read_csv(
        chartevents,
        usecols=usecols_char,
        chunksize=chunksize,
        low_memory=False
    ), start=1):

        print(f"  Lendo chunk chartevents {i}...")

        chunk[COL_HADM_ID] = pd.to_numeric(chunk[COL_HADM_ID], errors="coerce")
        chunk["itemid"] = pd.to_numeric(chunk["itemid"], errors="coerce")
        chunk["valuenum"] = pd.to_numeric(chunk["valuenum"], errors="coerce")
        chunk["charttime"] = pd.to_datetime(chunk["charttime"], errors="coerce")

        chunk = chunk.dropna(subset=[COL_HADM_ID, "itemid", "charttime", "valuenum"])
        chunk = filtrar_para_hadm_ids_base(chunk, hadm_ids_base)
        chunk = chunk[chunk["itemid"].isin(char_itemids)].copy()

        if chunk.empty:
            continue

        chunk[COL_HADM_ID] = chunk[COL_HADM_ID].astype(int)

        # Agora usa a data real do evento, sem calcular desde a intubação
        chunk[COL_DATA_DIA] = chunk["charttime"].dt.date

        lista_char.append(
            chunk[[COL_HADM_ID, COL_DATA_DIA, "itemid", "charttime", "valuenum"]]
        )

    if lista_char:
        df_char = pd.concat(lista_char, ignore_index=True)
        print(f"Total de linhas úteis em chartevents: {len(df_char):,}")
    else:
        df_char = pd.DataFrame(
            columns=[COL_HADM_ID, COL_DATA_DIA, "itemid", "charttime", "valuenum"]
        )
        print("Nenhuma linha útil encontrada em chartevents.")

    for var in char_vars_validas:
        itemid = var["itemid"]
        nome_col = var["coluna"]

        temp = df_char[df_char["itemid"] == itemid].copy()

        if temp.empty:
            print(f"  itemid {itemid} sem dados em chartevents.")
            continue

        eventos_df = gerar_eventos_agregados(
            df=temp,
            group_cols=[COL_HADM_ID, COL_DATA_DIA],
            value_col="valuenum",
            itemid_col="itemid",
            nome_procedure=nome_col
        )

        eventos_final.append(eventos_df)

        print(f"  eventos adicionados: {nome_col} (itemid {itemid})")

else:
    print("\nNenhum itemid válido definido para chartevents.")

# =========================================================
# 7) PROCESSAR INPUTEVENTS
# =========================================================

input_vars_validas = [v for v in INPUTEVENTS_VARS if v["itemid"] != 0]
input_itemids = sorted(set(v["itemid"] for v in input_vars_validas))

if input_itemids:
    print("\nProcessando inputevents...")

    usecols_input = [COL_HADM_ID, "itemid", "starttime", "amount"]
    lista_input = []
    chunksize = 500_000

    for i, chunk in enumerate(pd.read_csv(
        inputevents,
        usecols=usecols_input,
        chunksize=chunksize,
        low_memory=False
    ), start=1):

        print(f"  Lendo chunk inputevents {i}...")

        chunk[COL_HADM_ID] = pd.to_numeric(chunk[COL_HADM_ID], errors="coerce")
        chunk["itemid"] = pd.to_numeric(chunk["itemid"], errors="coerce")
        chunk["amount"] = pd.to_numeric(chunk["amount"], errors="coerce")
        chunk["starttime"] = pd.to_datetime(chunk["starttime"], errors="coerce")

        chunk = chunk.dropna(subset=[COL_HADM_ID, "itemid", "starttime", "amount"])
        chunk = filtrar_para_hadm_ids_base(chunk, hadm_ids_base)
        chunk = chunk[chunk["itemid"].isin(input_itemids)].copy()

        if chunk.empty:
            continue

        chunk[COL_HADM_ID] = chunk[COL_HADM_ID].astype(int)

        # Agora usa a data real do evento, sem calcular desde a intubação
        chunk[COL_DATA_DIA] = chunk["starttime"].dt.date

        lista_input.append(
            chunk[[COL_HADM_ID, COL_DATA_DIA, "itemid", "starttime", "amount"]]
        )

    if lista_input:
        df_input = pd.concat(lista_input, ignore_index=True)
        print(f"Total de linhas úteis em inputevents: {len(df_input):,}")
    else:
        df_input = pd.DataFrame(
            columns=[COL_HADM_ID, COL_DATA_DIA, "itemid", "starttime", "amount"]
        )
        print("Nenhuma linha útil encontrada em inputevents.")

    for var in input_vars_validas:
        itemid = var["itemid"]
        nome_col = var["coluna"]

        temp = df_input[df_input["itemid"] == itemid].copy()

        if temp.empty:
            print(f"  itemid {itemid} sem dados em inputevents.")
            continue

        temp = temp.rename(columns={
            "amount": "valuenum",
            "starttime": "charttime"
        })

        eventos_df = gerar_eventos_agregados(
            df=temp,
            group_cols=[COL_HADM_ID, COL_DATA_DIA],
            value_col="valuenum",
            itemid_col="itemid",
            nome_procedure=nome_col
        )

        eventos_final.append(eventos_df)

        print(f"  eventos adicionados: {nome_col} (itemid {itemid})")

else:
    print("\nNenhum itemid válido definido para inputevents.")

# =========================================================
# 8) PROCESSAR LABEVENTS
# =========================================================

print("\nCalculando presença de dados por coluna...")

lab_vars_validas = [v for v in LABEVENTS_VARS if v["itemid"] != 0]

lab_itemids = set()

for v in lab_vars_validas:
    ids = v["itemid"]

    if isinstance(ids, list):
        lab_itemids.update(ids)
    else:
        lab_itemids.add(ids)

lab_itemids = sorted(lab_itemids)

if lab_itemids:
    print("\nProcessando labevents...")

    usecols_lab = [COL_HADM_ID, "itemid", "charttime", "valuenum"]
    lista_lab = []
    chunksize = 1_000_000

    for i, chunk in enumerate(pd.read_csv(
        labevents,
        usecols=usecols_lab,
        chunksize=chunksize,
        low_memory=False
    ), start=1):

        print(f"  Lendo chunk labevents {i}...")

        chunk[COL_HADM_ID] = pd.to_numeric(chunk[COL_HADM_ID], errors="coerce")
        chunk["itemid"] = pd.to_numeric(chunk["itemid"], errors="coerce")
        chunk["valuenum"] = pd.to_numeric(chunk["valuenum"], errors="coerce")
        chunk["charttime"] = pd.to_datetime(chunk["charttime"], errors="coerce")

        chunk = chunk.dropna(subset=[COL_HADM_ID, "itemid", "charttime", "valuenum"])
        chunk = filtrar_para_hadm_ids_base(chunk, hadm_ids_base)
        chunk = chunk[chunk["itemid"].isin(lab_itemids)].copy()

        if chunk.empty:
            continue

        chunk[COL_HADM_ID] = chunk[COL_HADM_ID].astype(int)

        # Agora usa a data real do evento, sem calcular desde a intubação
        chunk[COL_DATA_DIA] = chunk["charttime"].dt.date

        lista_lab.append(
            chunk[[COL_HADM_ID, COL_DATA_DIA, "itemid", "charttime", "valuenum"]]
        )

    if lista_lab:
        df_lab = pd.concat(lista_lab, ignore_index=True)
        print(f"Total de linhas úteis em labevents: {len(df_lab):,}")
    else:
        df_lab = pd.DataFrame(
            columns=[COL_HADM_ID, COL_DATA_DIA, "itemid", "charttime", "valuenum"]
        )
        print("Nenhuma linha útil encontrada em labevents.")

    for var in lab_vars_validas:
        itemid = var["itemid"]
        nome_col = var["coluna"]

        if isinstance(itemid, list):
            temp = df_lab[df_lab["itemid"].isin(itemid)].copy()
        else:
            temp = df_lab[df_lab["itemid"] == itemid].copy()

        if temp.empty:
            print(f"  itemid {itemid} sem dados em labevents.")
            continue

        eventos_df = gerar_eventos_agregados(
            df=temp,
            group_cols=[COL_HADM_ID, COL_DATA_DIA],
            value_col="valuenum",
            itemid_col="itemid",
            nome_procedure=nome_col
        )

        eventos_final.append(eventos_df)

        print(f"  eventos adicionados: {nome_col} (itemid {itemid})")

else:
    print("\nNenhum itemid válido definido para labevents.")

# =========================================================
# 9) SALVAR OUTPUT LONGO
# =========================================================

if eventos_final:
    tabela_eventos = pd.concat(eventos_final, ignore_index=True)
else:
    tabela_eventos = pd.DataFrame(columns=[
        "hadm_id",
        "time",
        "id_procedure",
        "procedure_name",
        "resultado",
        "min_dia",
        "max_dia",
        "med_dia"
    ])

tabela_eventos = tabela_eventos.sort_values(
    ["hadm_id", "time", "procedure_name", "id_procedure"]
).reset_index(drop=True)

tabela_eventos.to_csv(output, index=False)

print(f"\nArquivo final salvo em:\n{output}")
print(f"Total de linhas geradas: {len(tabela_eventos):,}")
print("Concluído.")
