import pandas as pd
from pathlib import Path
import re

# ===========================
# 1) CAMINHOS
# ===========================
desktop = Path.home() / "Desktop"

d_items_file       = desktop / "d_items.csv.gz"
procedureevents_file = desktop / "procedureevents.csv.gz"
diagnoses_icd_file = desktop / "diagnoses_icd.csv.gz"
d_icd_diagnoses_file = desktop / "d_icd_diagnoses.csv.gz"
admissions_file    = desktop / "admissions.csv.gz"


# =========================
# 2) FUNÇÃO AUXILIAR
# =========================

"""
Regex é uma linguagem para encontrar padrões dentro de textos. 
Em vez de você buscar apenas por uma palavra exata, o Regex permite buscar por "qualquer palavra de uma lista", 
"palavras que começam com tal letra", etc. No seu código, ele é usado para buscar várias palavras ao mesmo tempo.

* k.lower(): Transforma todas as suas palavras-chave (keywords) em minúsculas para padronizar.

* re.escape(k): Isso é uma segurança. Se sua palavra tiver um caractere especial (como um ponto ou parênteses), 
o re.escape avisa ao computador: "isso é apenas um texto, não um comando de programação".

* Isso diz ao Python: "Procure por sepse OU pneumonia".
"""

def has_any_keyword(series, keywords):
    pattern = "|".join(re.escape(k.lower()) for k in keywords)
    return (
        series.fillna("")
        .astype(str)
        .str.lower()
        .str.contains(pattern, na=False, regex=True)
    )

# =========================
# 3) LER ARQUIVOS
# =========================
d_items = pd.read_csv(d_items_file, compression="gzip", usecols=["itemid", "label"])

proc = pd.read_csv(
    procedureevents_file,
    compression="gzip",
    usecols=["hadm_id", "itemid", "starttime", "endtime"]
)

diagnoses = pd.read_csv(diagnoses_icd_file, compression="gzip")

d_icd = pd.read_csv(d_icd_diagnoses_file, compression="gzip")

admissions = pd.read_csv(admissions_file, compression="gzip", usecols=["hadm_id", "hospital_expire_flag"])


# =========================
# 4) IDENTIFICAR ENTUBADOS E EXTUBADOS
# =========================
intub_keywords = [
    "intubat",
    "endotracheal",
    "ett",
    "mechanical ventilation",
    "ventilator",
    "invasive vent"
]

excluir_keywords = [
    "atrial",
    "ventricular",
    "pacemaker",
    "temporary"
]

extub_keywords = [
    "extubat",
    "extubation",
    "self extubation",
    "unplanned extubation",
    "ett removed"
]

d_items["label_lower"] = d_items["label"].fillna("").astype(str).str.lower()    # transforma tudo em letras minísculas 

mask_intub   = d_items["label_lower"].str.contains("|".join(intub_keywords), na=False)  # marca como verdadeiro todas as linhas que tem intub_keywords
mask_excluir = d_items["label_lower"].str.contains("|".join(excluir_keywords), na=False)   # marca como verdadeiro o que eu não quero também 
pattern_extub = "|".join(re.escape(k) for k in extub_keywords)

intub_itemids = set(d_items[mask_intub & ~mask_excluir]["itemid"]) # set é uma lista mais eficiente 

extub_itemids = set(
    d_items.loc[
        d_items["label_lower"].str.contains(pattern_extub, na=False),
        "itemid"
    ]
)

entubados_hadm = proc.loc[proc["itemid"].isin(intub_itemids), "hadm_id"].dropna().unique() # anota os hadm_ids qde pessoas que tem intub_items dentro de item_ids - remove linhas vazias
                                                                                           # e tira duplicatas (não duas intubações, dois registro exatemente idênticos)

# Extubações e reintubações
extub_events = proc[proc["itemid"].isin(extub_itemids)].copy()
extub_events_clean = extub_events.drop_duplicates(subset=["hadm_id", "starttime", "endtime"])
extub_counts = extub_events_clean.groupby("hadm_id").size()

ids_com_extubacao = set(extub_counts.index)
ids_reintubados   = set(extub_counts[extub_counts > 1].index)


# =========================================
# 5) FILTRAR DIAGNÓSTICOS DOS ENTUBADOS
# =========================================
diagnoses_entubados = diagnoses[diagnoses["hadm_id"].isin(entubados_hadm)].copy()
diag_full = diagnoses_entubados.merge(d_icd, on=["icd_code", "icd_version"], how="left")

seq1 = diag_full[diag_full["seq_num"] == 1].copy()
seq2 = diag_full[diag_full["seq_num"] == 2].copy()


# =====================================================
# 6) GRUPO SEPSE (seq_num=1) + RESPIRATÓRIO (seq_num=2)
# =====================================================
sepsis_keywords = ["sepsis", "septic", "severe sepsis", "septic shock"]

resp_keywords = [
    "respiratory", "pneumonia", "pulmonary", "resp failure", "respiratory failure",
    "acute respiratory failure", "chronic respiratory failure", "hypox", "hypercap",
    "copd", "asthma", "bronch", "lung", "pleura", "pleural", "atelect",
    "aspiration", "ards", "adult respiratory distress", "emphysema", "dyspnea"
]

seq1_sepsis = seq1[has_any_keyword(seq1["long_title"], sepsis_keywords)].copy()
seq2_resp   = seq2[has_any_keyword(seq2["long_title"], resp_keywords)].copy()

df_sepse_resp = seq1_sepsis[["hadm_id"]].merge(
    seq2_resp[["hadm_id", "long_title"]],
    on="hadm_id",
    how="inner"
)

excluir_seq2 = [
    "Abscess of lung",
    "Acute edema of lung, unspecified",
    "Acute postprocedural respiratory failure",
    "Acute pulmonary edema",
    "Acute respiratory failure following trauma and surgery",
    "Candidiasis of lung",
    "Gangrene and necrosis of lung",
    "Invasive pulmonary aspergillosis",
    "Other pulmonary embolism and infarction",
    "Other pulmonary embolism with acute cor pulmonale",
    "Other pulmonary embolism without acute cor pulmonale",
    "Other pulmonary insufficiency, not elsewhere classified, following trauma and surgery",
    "Pulmonary insufficiency following trauma and surgery",
    "Septic pulmonary embolism without acute cor pulmonale"
]

df_sepse_resp = df_sepse_resp[~df_sepse_resp["long_title"].isin(excluir_seq2)].copy()

grupo_pneumonia = [
    "Bacterial pneumonia, unspecified",
    "Bronchopneumonia, organism unspecified",
    "Influenza due to identified avian influenza virus with pneumonia",
    "Influenza due to other identified influenza virus with other specified pneumonia",
    "Influenza due to other identified influenza virus with unspecified type of pneumonia",
    "Influenza due to unidentified influenza virus with specified pneumonia",
    "Influenza with pneumonia",
    "Lobar pneumonia, unspecified organism",
    "Methicillin resistant pneumonia due to Staphylococcus aureus",
    "Methicillin susceptible pneumonia due to Staphylococcus aureus",
    "Other pneumonia, unspecified organism",
    "Pneumococcal pneumonia [Streptococcus pneumoniae pneumonia]",
    "Pneumonia due to Escherichia coli",
    "Pneumonia due to Hemophilus influenzae",
    "Pneumonia due to Hemophilus influenzae [H. influenzae]",
    "Pneumonia due to Klebsiella pneumoniae",
    "Pneumonia due to Legionnaires' disease",
    "Pneumonia due to Methicillin resistant Staphylococcus aureus",
    "Pneumonia due to Methicillin susceptible Staphylococcus aureus",
    "Pneumonia due to Pseudomonas",
    "Pneumonia due to Streptococcus pneumoniae",
    "Pneumonia due to escherichia coli [E. coli]",
    "Pneumonia due to other Gram-negative bacteria",
    "Pneumonia due to other gram-negative bacteria",
    "Pneumonia due to other streptococci",
    "Pneumonia in aspergillosis",
    "Pneumonia in other systemic mycoses",
    "Pneumonia, organism unspecified",
    "Pneumonia, unspecified organism",
    "Unspecified bacterial pneumonia",
    "Viral pneumonia, unspecified"
]

grupo_resp_failure = [
    "Acute and chronic respiratory failure",
    "Acute and chronic respiratory failure with hypercapnia",
    "Acute and chronic respiratory failure with hypoxia",
    "Acute and chronic respiratory failure, unspecified whether with hypoxia or hypercapnia",
    "Acute respiratory distress syndrome",
    "Acute respiratory failure",
    "Acute respiratory failure with hypercapnia",
    "Acute respiratory failure with hypoxia",
    "Acute respiratory failure, unspecified whether with hypoxia or hypercapnia",
    "Other pulmonary insufficiency, not elsewhere classified",
    "Pulmonary eosinophilia",
    "Respiratory failure, unspecified with hypercapnia",
    "Respiratory failure, unspecified with hypoxia",
    "Respiratory failure, unspecified, unspecified whether with hypoxia or hypercapnia",
    "Septic pulmonary embolism"
]

df_sepse_resp["diagnostico"] = None
df_sepse_resp.loc[df_sepse_resp["long_title"].isin(grupo_pneumonia),    "diagnostico"] = "sepse_pneumonia"
df_sepse_resp.loc[df_sepse_resp["long_title"].isin(grupo_resp_failure),  "diagnostico"] = "sepse_resp_failure"
df_sepse_resp = df_sepse_resp[df_sepse_resp["diagnostico"].notna()][["hadm_id", "diagnostico"]].copy()


# =====================================================
# 7) GRUPO RESPIRATÓRIO PRIMÁRIO (seq_num=1)
# =====================================================
grupo_obstructive_chronic_bronchitis = [
    "Obstructive chronic bronchitis",
    "Obstructive chronic bronchitis with acute bronchitis",
    "Obstructive chronic bronchitis with acute exacerbation",
    "Obstructive chronic bronchitis without exacerbation"
]

df_seq1 = seq1[["hadm_id", "long_title"]].copy()
df_seq1["diagnostico"] = None
df_seq1.loc[df_seq1["long_title"].isin(grupo_resp_failure),              "diagnostico"] = "acute_respiratory_failure"
df_seq1.loc[df_seq1["long_title"].isin(grupo_pneumonia),                 "diagnostico"] = "pneumonia"
df_seq1.loc[df_seq1["long_title"].isin(grupo_obstructive_chronic_bronchitis), "diagnostico"] = "obstructive_chronic_bronchitis"
df_seq1 = df_seq1[df_seq1["diagnostico"].notna()][["hadm_id", "diagnostico"]].copy()


# =========================
# 8) MONTAR BASE E FILTRAR
# =========================
pacientes_base = pd.concat([df_sepse_resp, df_seq1], ignore_index=True)
pacientes_base = pacientes_base.merge(admissions, on="hadm_id", how="left")
pacientes_base["morte"] = pacientes_base["hospital_expire_flag"].fillna(0).astype(int)

# Remove quem não tem extubação registrada e não morreu (dado incompleto)
final_df = pacientes_base[
    pacientes_base["hadm_id"].isin(ids_com_extubacao) | (pacientes_base["morte"] == 1)
].copy()


# =========================
# 9) LIMPEZA FINAL
# =========================
ids_excluir = [20451446, 24083260, 27561156, 25377349, 26016930, 23974949, 23911112, 22911229, 27680349, 29745972] # inconsistências temporais 

final_df = final_df[~final_df["hadm_id"].isin(ids_excluir)].copy()
final_df = final_df.drop_duplicates(subset=["hadm_id", "diagnostico"]).reset_index(drop=True)

# Coluna reintubacao
final_df["reintubacao"] = final_df["hadm_id"].isin(ids_reintubados).astype(int)

# Remover colunas auxiliares
colunas_para_remover = ["hospital_expire_flag", "long_title"]
final_df = final_df.drop(columns=[c for c in colunas_para_remover if c in final_df.columns])


# =========================
# 10) SALVAR CSV
# =========================
output_file = desktop / "pacientes.csv"
final_df.to_csv(output_file, index=False)

print(f"CSV salvo em: {output_file}")
print(final_df["diagnostico"].value_counts())
print(final_df.head())
print(f"Total de linhas: {len(final_df)}")
print(f"Total de hadm_id únicos: {final_df['hadm_id'].nunique()}")
