"""
MIMIC-IV Full Hospital Time Events Builder
==========================================
Colunas:
    hadm_id | subject_id | source_table | itemid | event_label | input_type |
    starttime | endtime | is_extubation | is_death | value
"""

import os
import pandas as pd

# ══════════════════════════════════════════════════════
DATA_DIR     = "/Users/giuliamello/Desktop/PROJ MIMIC/mimic-iv-moscal"
OUT_PATH     = "/Users/giuliamello/Desktop/PROJ MIMIC/time_events.csv"
PACIENTES_PATH = "/Users/giuliamello/Desktop/PROJ MIMIC/pacientes.csv"
# ══════════════════════════════════════════════════════

def load_gz(filename, usecols=None):
    path = os.path.join(DATA_DIR, filename)
    print(f"  Carregando {filename} ...")
    return pd.read_csv(path, compression="gzip", usecols=usecols, low_memory=False)

# ── Carrega coorte de pacientes ───────────────────────────────────────────────
print("\n[0/6] Carregando coorte de pacientes")
pacientes = pd.read_csv(PACIENTES_PATH)
HADM_IDS = set(pacientes["hadm_id"].astype(int))
print(f"  {len(HADM_IDS):,} hadm_id únicos na coorte")

# ── Item IDs ─────────────────────────────────────────────────────────────────
INTUBATION_ITEMS  = {224385, 226237, 225792}
EXTUBATION_ITEMS  = {225793, 225468, 225477, 227194}
VASOPRESSOR_ITEMS = {221906, 221289, 222315, 221662, 221653, 229617}
FLUID_ITEMS       = {220949, 220950, 225158, 225159, 225161, 225828, 225827, 226364, 226452, 225174, 220862}
ALL_PROC_INPUT    = INTUBATION_ITEMS | EXTUBATION_ITEMS | VASOPRESSOR_ITEMS | FLUID_ITEMS

def classify_input_type(source_table, itemid):
    if source_table == "emar":                     return "medication"
    if source_table == "procedureevents":          return "procedure"
    if source_table in ("admissions", "icustays"): return "milestone"
    if pd.isna(itemid):                            return "-"
    itemid = int(itemid)
    if itemid in FLUID_ITEMS:                      return "fluid"
    if itemid in VASOPRESSOR_ITEMS:                return "drug"
    return "-"

# ── 1. Admissões ─────────────────────────────────────────────────────────────
print("\n[1/6] Admissões")
adm = load_gz("admissions.csv.gz", usecols=["hadm_id", "subject_id", "admittime", "dischtime", "deathtime", "hospital_expire_flag"])
adm = adm[adm["hadm_id"].isin(HADM_IDS)].copy()  # filtra coorte
adm["admittime"] = pd.to_datetime(adm["admittime"])
adm["dischtime"] = pd.to_datetime(adm["dischtime"])
adm["deathtime"] = pd.to_datetime(adm["deathtime"])
adm["is_death"]  = adm["hospital_expire_flag"].fillna(0).astype(bool)
adm_idx = adm.set_index("hadm_id")

# Marco: hospital_admission
hosp_adm = pd.DataFrame({
    "hadm_id":       adm["hadm_id"],
    "subject_id":    adm["subject_id"],
    "source_table":  "admissions",
    "itemid":        None,
    "event_label":   "hospital_admission",
    "starttime":     adm["admittime"],
    "endtime":       pd.NaT,
    "is_extubation": False,
    "is_death":      adm["is_death"],
    "value":         None,
    "valueuom":      None,
})

# Marco: hospital_discharge
hosp_dis = pd.DataFrame({
    "hadm_id":       adm["hadm_id"],
    "subject_id":    adm["subject_id"],
    "source_table":  "admissions",
    "itemid":        None,
    "event_label":   "hospital_discharge",
    "starttime":     adm["dischtime"],
    "endtime":       pd.NaT,
    "is_extubation": False,
    "is_death":      adm["is_death"],
    "value":         None,
    "valueuom":      None,
})

# Marco: death (só quem morreu com deathtime registrado)
adm_death = adm[adm["is_death"] & adm["deathtime"].notna()]
hosp_death = pd.DataFrame({
    "hadm_id":       adm_death["hadm_id"],
    "subject_id":    adm_death["subject_id"],
    "source_table":  "admissions",
    "itemid":        None,
    "event_label":   "death",
    "starttime":     adm_death["deathtime"],
    "endtime":       pd.NaT,
    "is_extubation": False,
    "is_death":      True,
    "value":         None,
    "valueuom":      None,
})

# ── 2. ICU stays ─────────────────────────────────────────────────────────────
print("\n[2/6] ICU stays")
icu = load_gz("icustays.csv.gz", usecols=["hadm_id", "subject_id", "intime", "outtime"])
icu = icu[icu["hadm_id"].isin(HADM_IDS)].copy()  # filtra coorte
icu["intime"]  = pd.to_datetime(icu["intime"])
icu["outtime"] = pd.to_datetime(icu["outtime"])

# Marco: icu_admission (uma linha por estadia na UTI)
icu_adm = pd.DataFrame({
    "hadm_id":       icu["hadm_id"],
    "subject_id":    icu["subject_id"],
    "source_table":  "icustays",
    "itemid":        None,
    "event_label":   "icu_admission",
    "starttime":     icu["intime"],
    "endtime":       pd.NaT,
    "is_extubation": False,
    "is_death":      False,
    "value":         None,
    "valueuom":      None,
})

# Marco: icu_discharge
icu_dis = pd.DataFrame({
    "hadm_id":       icu["hadm_id"],
    "subject_id":    icu["subject_id"],
    "source_table":  "icustays",
    "itemid":        None,
    "event_label":   "icu_discharge",
    "starttime":     icu["outtime"],
    "endtime":       pd.NaT,
    "is_extubation": False,
    "is_death":      False,
    "value":         None,
    "valueuom":      None,
})

# ── 3. d_items ───────────────────────────────────────────────────────────────
print("\n[3/6] d_items")
d_items = load_gz("d_items.csv.gz", usecols=["itemid", "label"])
label_map = d_items.set_index("itemid")["label"].to_dict()

# ── 4. inputevents ───────────────────────────────────────────────────────────
print("\n[4/6] inputevents")
inp = load_gz("inputevents.csv.gz", usecols=["hadm_id", "itemid", "starttime", "endtime", "amount", "amountuom"])
inp = inp[inp["hadm_id"].isin(HADM_IDS) & inp["itemid"].isin(ALL_PROC_INPUT)].copy()
inp["starttime"]    = pd.to_datetime(inp["starttime"])
inp["endtime"]      = pd.to_datetime(inp["endtime"])
inp.rename(columns={"amount": "value", "amountuom": "valueuom"}, inplace=True)
inp["source_table"] = "inputevents"
inp["is_extubation"]= False
inp["is_death"]     = False
inp["event_label"]  = inp["itemid"].map(label_map)
inp["subject_id"]   = inp["hadm_id"].map(adm_idx["subject_id"])

# ── 5. procedureevents ───────────────────────────────────────────────────────
print("\n[5/6] procedureevents")
proc = load_gz("procedureevents.csv.gz", usecols=["hadm_id", "itemid", "starttime", "endtime", "value", "valueuom"])
proc = proc[proc["hadm_id"].isin(HADM_IDS) & proc["itemid"].isin(ALL_PROC_INPUT)].copy()
proc["starttime"]    = pd.to_datetime(proc["starttime"])
proc["endtime"]      = pd.to_datetime(proc["endtime"])
proc["source_table"] = "procedureevents"
proc["is_extubation"]= proc["itemid"].isin(EXTUBATION_ITEMS)
proc["is_death"]     = False
proc["event_label"]  = proc["itemid"].map(label_map)
proc.loc[proc["itemid"].isin(INTUBATION_ITEMS), "event_label"] = "Invasive Ventilation"  # CLASSIFICA OS 3 IDS COMO INVASIVE VENTILATION
proc.loc[proc["itemid"].isin(EXTUBATION_ITEMS), "event_label"] = "Extubation"   # FAZ A MESMA COISA COM EXTUBATION 
proc["subject_id"]   = proc["hadm_id"].map(adm_idx["subject_id"])

# ── 6. emar ──────────────────────────────────────────────────────────────────
print("\n[6/6] emar")
emar = load_gz("emar.csv.gz", usecols=["hadm_id", "subject_id", "charttime", "scheduletime", "medication", "event_txt"])
emar = emar[emar["hadm_id"].isin(HADM_IDS)].copy()
emar.rename(columns={
    "charttime":    "starttime",
    "scheduletime": "endtime",
    "medication":   "event_label",
    "event_txt":    "value"
}, inplace=True)
emar["starttime"]    = pd.to_datetime(emar["starttime"])
emar["endtime"]      = pd.to_datetime(emar["endtime"])
emar["valueuom"]     = None
emar["source_table"] = "emar"
emar["itemid"]       = None
emar["is_extubation"]= False
emar["is_death"]     = False

# ── Concatena tudo ────────────────────────────────────────────────────────────
print("\nConcatenando eventos...")
all_events = pd.concat(
    [hosp_adm, hosp_dis, hosp_death, icu_adm, icu_dis, inp, proc, emar],
    ignore_index=True
)

# ── Filtra eventos clínicos fora da janela de internação ─────────────────────
print("Filtrando janela de internação...")
all_events = all_events.merge(
    adm[["hadm_id", "admittime", "dischtime", "deathtime"]], on="hadm_id", how="left"
)
end_window = all_events["dischtime"].fillna(all_events["deathtime"])
is_milestone = all_events["source_table"].isin(["admissions", "icustays"])
in_window = (all_events["starttime"] >= all_events["admittime"]) & (all_events["starttime"] <= end_window)
all_events = all_events[is_milestone | in_window].copy()
all_events.drop(columns=["admittime", "dischtime", "deathtime"], inplace=True)

# ── input_type ────────────────────────────────────────────────────────────────
all_events["input_type"] = all_events.apply(
    lambda r: classify_input_type(r["source_table"], r["itemid"]), axis=1
)

# ── Formata datas ─────────────────────────────────────────────────────────────
fmt = "%d/%m/%Y %H:%M:%S"
for col in ["starttime", "endtime"]:
    all_events[col] = pd.to_datetime(all_events[col], errors="coerce").dt.strftime(fmt)

# ── Colunas finais ────────────────────────────────────────────────────────────
final_cols = [
    "hadm_id", "subject_id", "source_table",
    "itemid", "event_label", "input_type",
    "starttime", "endtime",
    "is_extubation", "is_death",
    "value", "valueuom"
]
out = all_events[final_cols].sort_values(["hadm_id", "starttime"]).reset_index(drop=True)

# ── Exporta ───────────────────────────────────────────────────────────────────
out.to_csv(OUT_PATH, index=False)
print(f"\n✅ Arquivo salvo: {OUT_PATH}")
print(f"   Linhas:         {len(out):,}")
print(f"   hadm_id únicos: {out['hadm_id'].nunique():,}")
print(f"   Milestones:     {out[out['source_table'].isin(['admissions','icustays'])].shape[0]:,}")
print(f"   Extubações:     {out['is_extubation'].sum():,}")
print(f"   Óbitos:         {out[out['event_label']=='death'].shape[0]:,}")
print(f"\nPrimeiras linhas:")
print(out.head(8).to_string())
