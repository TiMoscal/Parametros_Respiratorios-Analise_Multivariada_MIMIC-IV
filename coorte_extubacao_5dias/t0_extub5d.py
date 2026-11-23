#!/usr/bin/env python3
"""
Pipeline de pré-processamento — coorte de extubação (MIMIC-IV, IRA e Sepse + IRA)
=================================================================================

T=0 de cada paciente = primeira extubação registrada.

Ordem das etapas (cada uma registra pacientes retidos/excluídos):
  Passo 1  Define T=0, morte e reintubação; exclui quem não tem extubação.
  Passo 1b Exclui pacientes com mais de uma intubação ANTES do T=0.
  Passo 2  Regra 1 — óbito ≤ 12 h após o T=0 (ou antes do T=0).
  Passo 3  Regra 2 — morte ou reintubação > 120 h após o T=0.
  Passo 4  Regra 3 — remove linhas com Tempo_desde_Extubacao > 120 h.

Saídas (todas com sufixo _10dias):
  pacientes_10dias.csv, resultados_pacientes_10dias.csv, time_events_10dias.csv,
  auditoria_pacientes_10dias.csv, log_exclusoes_10dias.csv,
  volume_linhas_10dias.csv, estatisticas_excluidos_10dias.csv,
  graficos/*.png

Como rodar:
  pip install pandas numpy matplotlib scipy
  python t0_extub5d.py                      # padrão: lê ../CBEB-02, salva em ./resultados
  python t0_extub5d.py --dados <pasta> --saida <pasta>

A pasta de dados precisa ter (o .csv ou o .zip, tanto faz):
  pacientes.csv
  time_events_SEPSE_CBEB.csv(.zip)
  resultados_pacientes_CBEB.csv(.zip)
"""
import sys as _sys; from pathlib import Path as _Path; _sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))
try:
    import config  # caminhos do projeto local: ver ../config.py
    PADRAO_DADOS, PADRAO_SAIDA = str(config.BASE_CBEB), str(config.RES / "coorte_10dias")
except ImportError:  # rodando a partir do repositório GitHub (sem config.py)
    _AQUI = _Path(__file__).resolve().parent
    PADRAO_DADOS, PADRAO_SAIDA = str(_AQUI.parent / "CBEB-02"), str(_AQUI / "resultados")
import argparse
import glob
import os
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    from scipy import stats as sps
except ImportError:  # estatísticas rodam sem p-valor se não houver scipy
    sps = None


# =============================================================================
# CONFIGURAÇÃO — ajuste aqui
# =============================================================================
CONFIG = dict(
    janela_obito_precoce_h=12,      # Regra 1
    janela_desfecho_h=120,          # Regras 2 e 3 (5 dias)
    # Episódios de "Invasive Ventilation" que começam até X h depois do fim do
    # anterior (ou sobrepostos) são o mesmo episódio de intubação. Os registros
    # do MIMIC têm muita sobreposição/duplicata, então X=0 superestima.
    tolerancia_mesma_intubacao_h=1,
    # True  -> exclui quem teve >1 intubação ANTES do T=0 (recomendado).
    # "total" -> exclui quem teve >1 intubação em qualquer momento. ATENÇÃO: isso
    #            elimina todos os reintubados (reintubação = 2ª intubação).
    # False -> não aplica o critério.
    excluir_multiplas_intubacoes=True,
    # Regra 2 literal (False): exclui se morte OU reintubação > 120 h, mesmo
    # que o outro desfecho tenha ocorrido antes. True: olha só o 1º desfecho.
    regra2_apenas_primeiro_desfecho=False,
    pasta_saida="Dados_Processados_10dias",
    sufixo="_10dias",
)

# Faixas plausíveis — usadas SÓ nas estatísticas dos excluídos (os dados
# exportados ficam como vieram).
LIMITES = {
    "HR": (20, 250), "RR": (0, 80), "ABPs": (40, 300), "ABPd": (10, 200),
    "ABPm": (20, 250), "Temperature F": (77, 113), "FiO2": (21, 100),
    "PEEP": (0, 30), "SaO2": (50, 100), "PO2 (Arterial)": (20, 700),
    "PCO2 (Arterial)": (10, 150), "PH (Arterial)": (6.5, 8.0),
    "Platelet Count": (1, 2000), "pO2_lab": (10, 700), "pH_lab": (6.5, 8.0),
    "CVP": (-5, 40),
}
VITAIS_STATS = [  # (procedure_name, rótulo)
    ("HR", "FC (bpm)"), ("RR", "FR (irpm)"), ("FiO2", "FiO2 (%)"),
    ("Temperature F", "Temperatura (°F)"), ("GCS - Motor Response", "GCS motor"),
    ("Lactate", "Lactato (mmol/L)"), ("Platelet Count", "Plaquetas (10³/µL)"),
    ("PH (Arterial)", "pH arterial"),
]
DX = {"acute_respiratory_failure": "IRA", "sepse_resp_failure": "Sepse + IRA"}
GRUPOS = {  # status_coorte -> rótulo
    "excluido_sem_extubacao": "Sem extubação",
    "excluido_multiplas_intubacoes": ">1 intubação pré-T0",
    "excluido_regra1_obito_precoce": "Regra 1 (óbito ≤12 h)",
    "excluido_regra2_desfecho_tardio": "Regra 2 (desfecho >120 h)",
    "incluido": "Coorte final",
}


# =============================================================================
# Leitura
# =============================================================================
def acha_arquivo(pasta, base):
    """Encontra base.csv ou base.csv.zip (aceita prefixos, ex.: 'abc123-base.csv').
    Também aceita o zip extraído numa pasta de mesmo nome (base.csv/base.csv)."""
    padroes = (f"{base}.csv", f"{base}.csv/{base}.csv", f"{base}.csv.zip",
               f"*{base}.csv", f"*{base}.csv.zip")
    for padrao in padroes:
        achados = sorted(p for p in glob.glob(os.path.join(pasta, padrao)) if os.path.isfile(p))
        if achados:
            return achados[0]
    sys.exit(f"ERRO: não encontrei {base}.csv(.zip) em {os.path.abspath(pasta)}")


def carregar(pasta):
    print(f"Lendo dados de {os.path.abspath(pasta)} ...")
    pac = pd.read_csv(acha_arquivo(pasta, "pacientes"), dtype={"reintubacao": str})
    ev = pd.read_csv(acha_arquivo(pasta, "time_events_SEPSE_CBEB"),
                     parse_dates=["starttime", "endtime"])
    ev["hadm_id"] = ev["hadm_id"].astype("Int64")
    for c in ("is_extubation", "is_death"):
        ev[c] = ev[c].astype(str).str.lower().eq("true")
    res = pd.read_csv(acha_arquivo(pasta, "resultados_pacientes_CBEB"), parse_dates=["time"])
    print(f"  pacientes: {len(pac):,} | eventos: {len(ev):,} | resultados: {len(res):,}")
    return pac, ev, res


# =============================================================================
# Passo 1 — T=0, morte, reintubação, nº de intubações
# =============================================================================
def episodios_intubacao(ev, tol_h):
    """Funde registros de 'Invasive Ventilation' sobrepostos/próximos em episódios."""
    iv = (ev[ev.event_label == "Invasive Ventilation"][["hadm_id", "starttime", "endtime"]]
          .dropna(subset=["starttime"]).sort_values(["hadm_id", "starttime"]).copy())
    iv["endtime"] = iv["endtime"].fillna(iv["starttime"])
    fim_ant = iv.groupby("hadm_id")["endtime"].transform(lambda s: s.cummax().shift())
    gap_h = (iv.starttime - fim_ant).dt.total_seconds() / 3600
    iv["novo"] = gap_h.isna() | (gap_h > tol_h)
    iv["ep"] = iv.groupby("hadm_id")["novo"].cumsum()
    return (iv.groupby(["hadm_id", "ep"])
              .agg(inicio=("starttime", "min"), fim=("endtime", "max"))
              .reset_index())


def definir_eventos(pac, ev, cfg):
    ext = ev[ev.is_extubation].groupby("hadm_id").starttime
    t0 = ext.min().rename("T0_extubacao")
    n_ext = ext.size().rename("n_extubacoes")
    morte = ev[ev.event_label == "death"].groupby("hadm_id").starttime.min().rename("data_morte")
    alta = (ev[ev.event_label == "hospital_discharge"].groupby("hadm_id")
            .starttime.min().rename("data_alta_hospitalar"))
    adm = (ev[ev.event_label == "hospital_admission"].groupby("hadm_id")
           .starttime.min().rename("data_admissao_hospitalar"))
    uti = ev[ev.event_label == "icu_admission"].groupby("hadm_id").starttime.min().rename("data_admissao_uti")

    # Reintubação = 1º início de registro de VM invasiva depois do T=0
    iv_raw = ev[ev.event_label == "Invasive Ventilation"][["hadm_id", "starttime"]].join(t0, on="hadm_id")
    reint = (iv_raw[iv_raw.starttime > iv_raw.T0_extubacao]
             .groupby("hadm_id").starttime.min().rename("data_reintubacao"))

    # Nº de intubações (episódios fundidos) antes do T=0 e no total
    eps = episodios_intubacao(ev, cfg["tolerancia_mesma_intubacao_h"]).join(t0, on="hadm_id")
    n_int_total = eps.groupby("hadm_id").size().rename("n_intubacoes_total")
    n_int_pre = (eps[eps.inicio <= eps.T0_extubacao].groupby("hadm_id").size()
                 .rename("n_intubacoes_pre_T0"))
    inicio_vm = eps.groupby("hadm_id").inicio.min().rename("inicio_1a_VM")

    P = pac.set_index("hadm_id").join(
        [t0, n_ext, morte, alta, adm, uti, reint, n_int_total, n_int_pre, inicio_vm])
    for c in ("n_extubacoes", "n_intubacoes_total", "n_intubacoes_pre_T0"):
        P[c] = P[c].fillna(0).astype(int)

    def horas(c, ref="T0_extubacao"):
        return (P[c] - P[ref]).dt.total_seconds() / 3600
    P["horas_ate_morte"] = horas("data_morte")
    P["horas_ate_reintubacao"] = horas("data_reintubacao")
    P["horas_ate_alta"] = horas("data_alta_hospitalar")
    P["horas_VM_ate_T0"] = horas("T0_extubacao", "inicio_1a_VM")
    P["los_hospitalar_dias"] = (P.data_alta_hospitalar - P.data_admissao_hospitalar).dt.total_seconds() / 86400
    return P, t0


# =============================================================================
# Passos 1–3 — exclusões de pacientes
# =============================================================================
def aplicar_exclusoes(P, cfg):
    log = []

    def registra(etapa, retidos, excl, motivo):
        log.append(dict(etapa=etapa, pacientes_retidos=len(retidos),
                        pacientes_excluidos=len(excl), motivo=motivo))
        print(f"  {etapa:<42} retidos={len(retidos):>5}  excluídos={len(excl):>4}  {motivo}")

    P["status_coorte"] = "incluido"
    todos = set(P.index)
    registra("0. Base original", todos, set(), "Todas as internações")

    # Passo 1
    sem_ext = set(P.index[P.T0_extubacao.isna()])
    c = todos - sem_ext
    P.loc[list(sem_ext), "status_coorte"] = "excluido_sem_extubacao"
    registra("1. Definição do T=0", c, sem_ext, "Sem evento de extubação")

    # Passo 1b
    modo = cfg["excluir_multiplas_intubacoes"]
    if modo:
        col = "n_intubacoes_total" if modo == "total" else "n_intubacoes_pre_T0"
        multi = set(P.loc[list(c)].index[P.loc[list(c), col] > 1])
        c -= multi
        P.loc[list(multi), "status_coorte"] = "excluido_multiplas_intubacoes"
        txt = "em toda a internação" if modo == "total" else "antes do T=0"
        registra("1b. Mais de uma intubação", c, multi,
                 f">1 intubação {txt} (tolerância {cfg['tolerancia_mesma_intubacao_h']} h)")
    sem_iv = (P.loc[list(c), "n_intubacoes_pre_T0"] == 0).sum()
    if sem_iv:
        print(f"     (aviso: {sem_iv} pacientes retidos têm extubação sem registro de VM antes)")

    # Passo 2 — Regra 1
    J1 = cfg["janela_obito_precoce_h"]
    hm = P.loc[list(c), "horas_ate_morte"]
    r1 = set(hm.index[hm <= J1])  # inclui negativos (morte antes do T=0)
    n_neg = int((hm < 0).sum())
    c -= r1
    P.loc[list(r1), "status_coorte"] = "excluido_regra1_obito_precoce"
    registra("2. Regra 1 — óbito precoce", c, r1, f"Morte ≤{J1} h ({n_neg} com morte antes do T=0)")

    # Passo 3 — Regra 2
    J = cfg["janela_desfecho_h"]
    sub = P.loc[list(c)]
    if cfg["regra2_apenas_primeiro_desfecho"]:
        primeiro = sub[["horas_ate_morte", "horas_ate_reintubacao"]].min(axis=1)
        mask = primeiro > J
    else:
        mask = (sub.horas_ate_morte > J) | (sub.horas_ate_reintubacao > J)
    r2 = set(sub.index[mask])
    c -= r2
    P.loc[list(r2), "status_coorte"] = "excluido_regra2_desfecho_tardio"
    registra("3. Regra 2 — desfecho tardio", c, r2, f"Morte ou reintubação >{J} h")

    # Motivo detalhado da Regra 2
    def motivo(r):
        m, ri = r.horas_ate_morte > J, r.horas_ate_reintubacao > J
        if m and ri:
            return f"morte e reintubação >{J} h"
        if ri:
            return f"reintubação >{J} h"
        if pd.notna(r.horas_ate_reintubacao):
            return f"morte >{J} h após reintubação ≤{J} h"
        return f"morte >{J} h"
    P["motivo_regra2"] = None
    if r2:
        P.loc[list(r2), "motivo_regra2"] = P.loc[list(r2)].apply(motivo, axis=1)

    # Desfecho em 5 dias (coorte)
    def desfecho(r):
        m = pd.notna(r.horas_ate_morte) and r.horas_ate_morte <= J
        ri = pd.notna(r.horas_ate_reintubacao) and r.horas_ate_reintubacao <= J
        if m and ri:
            return "Reintubado e óbito"
        if m:
            return "Óbito"
        if ri:
            return "Reintubado"
        return "Sobrevivente sem reintubação"
    coorte = sorted(c)
    P["desfecho_5dias"] = None
    P.loc[coorte, "desfecho_5dias"] = P.loc[coorte].apply(desfecho, axis=1)
    P["reintubacao_evento_5dias"] = P.desfecho_5dias.isin(["Reintubado", "Reintubado e óbito"]).astype("Int64")
    P["morte_5dias"] = P.desfecho_5dias.isin(["Óbito", "Reintubado e óbito"]).astype("Int64")
    P.loc[P.desfecho_5dias.isna(), ["reintubacao_evento_5dias", "morte_5dias"]] = pd.NA
    return P, coorte, log


# =============================================================================
# Passo 4 — delta temporal e truncamento
# =============================================================================
def delta_e_truncamento(res, ev, t0, coorte, log, cfg):
    J = cfg["janela_desfecho_h"]
    res = res.join(t0, on="hadm_id")
    res["Tempo_desde_Extubacao"] = (res.time - res.T0_extubacao).dt.total_seconds() / 3600
    ev = ev.join(t0, on="hadm_id")
    ev["Tempo_desde_Extubacao"] = (ev.starttime - ev.T0_extubacao).dt.total_seconds() / 3600
    ev["Tempo_fim_desde_Extubacao"] = (ev.endtime - ev.T0_extubacao).dt.total_seconds() / 3600

    res_c, ev_c = res[res.hadm_id.isin(coorte)], ev[ev.hadm_id.isin(coorte)]
    res_f = res_c[res_c.Tempo_desde_Extubacao.between(-J, J)]
    ev_f = ev_c[ev_c.Tempo_desde_Extubacao.between(-J, J)]
    vol = pd.Series(dict(
        resultados_original=len(res), eventos_original=len(ev),
        resultados_coorte_antes_corte=len(res_c), eventos_coorte_antes_corte=len(ev_c),
        resultados_final=len(res_f), eventos_final=len(ev_f)), name="linhas")
    motivo = (f"Linhas removidas: resultados {len(res_c) - len(res_f):,}, "
              f"eventos {len(ev_c) - len(ev_f):,}")
    log.append(dict(etapa="4. Regra 3 — truncamento", pacientes_retidos=len(coorte),
                    pacientes_excluidos=0, motivo=motivo))
    print(f"  {'4. Regra 3 — truncamento':<42} retidos={len(coorte):>5}  excluídos={0:>4}  {motivo}")
    return res, res_f.drop(columns="T0_extubacao"), ev_f.drop(columns="T0_extubacao"), vol


# =============================================================================
# Estatísticas dos pacientes excluídos
# =============================================================================
def mediana_iqr(s):
    s = s.dropna()
    if s.empty:
        return "–"
    q1, q2, q3 = s.quantile([.25, .5, .75])
    fmt = (lambda v: f"{v:.2f}") if abs(q2) < 10 else (lambda v: f"{v:.0f}") if abs(q2) >= 100 else (lambda v: f"{v:.1f}")
    return f"{fmt(q2)} [{fmt(q1)}–{fmt(q3)}]"


def vitais_primeiras_24h_uti(P, res):
    """Mediana por paciente de cada variável nas primeiras 24 h de UTI (comparável
    entre todos os grupos, inclusive quem nunca foi extubado)."""
    r = res[res.procedure_name.isin([v for v, _ in VITAIS_STATS])][["hadm_id", "time", "procedure_name", "resultado"]]
    r = r.join(P["data_admissao_uti"], on="hadm_id")
    h = (r.time - r.data_admissao_uti).dt.total_seconds() / 3600
    r = r[(h >= 0) & (h <= 24)]
    for k, (a, b) in LIMITES.items():
        r = r[~((r.procedure_name == k) & ((r.resultado < a) | (r.resultado > b)))]
    return r.groupby(["hadm_id", "procedure_name"]).resultado.median().unstack()


def estatisticas_excluidos(P, res, pasta):
    V = vitais_primeiras_24h_uti(P, res)
    D = P.join(V)
    D["grupo"] = D.status_coorte.map(GRUPOS)
    D["sepse"] = (D.diagnostico == "sepse_resp_failure").astype(int)
    D["reint_rotulo"] = (D.reintubacao == "1").astype(int)
    D["excluido"] = (D.status_coorte != "incluido").astype(int)
    ordem = [g for g in GRUPOS.values() if g in set(D.grupo)]
    cols = ordem[:-1] + ["Todos excluídos", "Coorte final"]
    linhas = []

    def bloco(nome, fn, teste=None):
        lin = {"variavel": nome}
        for g in ordem:
            lin[g] = fn(D[D.grupo == g])
        lin["Todos excluídos"] = fn(D[D.excluido == 1])
        if teste and sps is not None:
            lin["p (excluídos vs coorte)"] = teste()
        linhas.append(lin)

    def pct(col):
        return lambda d: f"{d[col].sum()} ({d[col].mean():.0%})" if len(d) else "–"

    def chi2(col):
        def f():
            tab = pd.crosstab(D.excluido, D[col])
            return f"{sps.chi2_contingency(tab)[1]:.3g}" if tab.shape == (2, 2) else "–"
        return f

    def mw(col):
        def f():
            a = D.loc[D.excluido == 1, col].dropna()
            b = D.loc[D.excluido == 0, col].dropna()
            return f"{sps.mannwhitneyu(a, b)[1]:.3g}" if len(a) > 2 and len(b) > 2 else "–"
        return f

    bloco("N pacientes", lambda d: f"{len(d)}")
    bloco("Sepse + IRA, n (%)", pct("sepse"), chi2("sepse"))
    bloco("Óbito hospitalar, n (%)", pct("morte"), chi2("morte"))
    bloco("Reintubação (rótulo original), n (%)", pct("reint_rotulo"), chi2("reint_rotulo"))
    bloco("Internação hospitalar, dias", lambda d: mediana_iqr(d.los_hospitalar_dias), mw("los_hospitalar_dias"))
    bloco("VM até a 1ª extubação, h", lambda d: mediana_iqr(d.horas_VM_ate_T0), mw("horas_VM_ate_T0"))
    bloco("Extubação → óbito, h", lambda d: mediana_iqr(d.horas_ate_morte.where(d.morte == 1)))
    for v, rot in VITAIS_STATS:
        if v in D:
            bloco(f"{rot} — 1as 24 h de UTI", lambda d, v=v: mediana_iqr(d[v]), mw(v))
    T = pd.DataFrame(linhas).set_index("variavel")
    T = T[[c for c in cols if c in T] + [c for c in T if c.startswith("p (")]]
    T.to_csv(os.path.join(pasta, f"estatisticas_excluidos{CONFIG['sufixo']}.csv"))

    print("\nEstatísticas — excluídos vs coorte final (mediana [IQR] ou n (%)):")
    with pd.option_context("display.max_columns", None, "display.width", 250,
                           "display.max_colwidth", 22):
        print(T.to_string())
    if sps is None:
        print("  (instale scipy para ver os p-valores)")
    return D


# =============================================================================
# Gráficos
# =============================================================================
SURF, INK, INK2, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#8a8984", "#e6e5e0"
BLUE, ORANGE, AQUA, YELLOW, GRAY = "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#c3c2b7"


def estilo():
    plt.rcParams.update({
        "figure.facecolor": SURF, "axes.facecolor": SURF, "savefig.facecolor": SURF,
        "font.size": 10.5, "text.color": INK, "axes.edgecolor": GRID,
        "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
        "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
        "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
        "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlelocation": "left",
        "axes.titlepad": 24,
    })


def boxplot_h(ax, dados, **kw):
    """Boxplot horizontal compatível com matplotlib antigo (<3.10) e novo."""
    try:
        return ax.boxplot(dados, orientation="horizontal", **kw)
    except TypeError:
        return ax.boxplot(dados, vert=False, **kw)


def subtitulo(ax, txt):
    ax.text(0, 1.02, txt, transform=ax.transAxes, color=INK2, fontsize=9.5, va="bottom")


def salva(fig, fig_dir, nome):
    fig.tight_layout()
    fig.savefig(os.path.join(fig_dir, nome), dpi=160)
    plt.close(fig)


def fmt_mil(v):
    return f"{v:,}".replace(",", ".")


def graficos(P, D, vol, cfg, pasta):
    estilo()
    F = os.path.join(pasta, "graficos")
    os.makedirs(F, exist_ok=True)
    J, J1 = cfg["janela_desfecho_h"], cfg["janela_obito_precoce_h"]
    st = P.status_coorte.value_counts()
    pac = P[P.status_coorte == "incluido"]
    nf, n0 = len(pac), len(P)

    # 1. Cascata
    passos = [("Base original", n0, "total")]
    for k, lab in [("excluido_sem_extubacao", "Passo 1\nsem extubação"),
                   ("excluido_multiplas_intubacoes", "Passo 1b\n>1 intubação"),
                   ("excluido_regra1_obito_precoce", f"Regra 1\nóbito ≤{J1} h"),
                   ("excluido_regra2_desfecho_tardio", f"Regra 2\ndesfecho >{J} h")]:
        if k in st or k != "excluido_multiplas_intubacoes" or cfg["excluir_multiplas_intubacoes"]:
            passos.append((lab, int(st.get(k, 0)), "drop"))
    passos.append(("Coorte final", nf, "total"))
    fig, ax = plt.subplots(figsize=(10, 5.2))
    nivel = n0
    for i, (lab, h, k) in enumerate(passos):
        if k == "total":
            base = 0
        else:
            nivel -= h
            base = nivel
        ax.bar(i, h, bottom=base, width=0.55, color=BLUE if k == "total" else ORANGE,
               edgecolor=SURF, linewidth=2)
        ax.text(i, base + h + n0 * 0.012, fmt_mil(h) if k == "total" else f"−{h}",
                ha="center", va="bottom", fontweight="bold")
        if i < len(passos) - 1:
            topo = base + h if k == "total" else base
            ax.plot([i + 0.28, i + 0.72], [topo, topo], color=MUTED, lw=1, ls=(0, (3, 2)))
    ax.set_xticks(range(len(passos)), [p[0] for p in passos])
    ax.set_ylabel("Pacientes (internações)")
    ax.set_ylim(0, n0 * 1.12)
    ax.grid(axis="x", visible=False)
    ax.set_title("Fluxo de pacientes até a coorte final")
    subtitulo(ax, f"{n0} internações → {nf} retidas ({nf / n0:.0%}). Azul = retidos; laranja = excluídos na etapa.")
    salva(fig, F, "01_cascata_pacientes.png")

    # 2. Histograma óbito precoce
    r1 = P[P.status_coorte == "excluido_regra1_obito_precoce"]
    if len(r1):
        fig, ax = plt.subplots(figsize=(9, 5))
        bins = np.arange(min(0, np.floor(r1.horas_ate_morte.min())), J1 + 1, 1)
        cnt, _, _ = ax.hist(r1.horas_ate_morte, bins=bins, color=ORANGE, edgecolor=SURF, linewidth=2)
        for x, cc in zip(bins[:-1], cnt):
            if cc:
                ax.text(x + 0.5, cc + 0.6, str(int(cc)), ha="center", va="bottom", fontsize=9, color=INK2)
        med = r1.horas_ate_morte.median()
        ax.axvline(med, color=INK2, lw=1.2, ls=(0, (4, 3)))
        ax.text(med + 0.12, ax.get_ylim()[1] * 0.93, f"mediana {med:.1f} h", color=INK2, fontsize=9)
        ax.set_xticks(bins.astype(int))
        ax.set_xlabel("Horas entre extubação (T=0) e óbito")
        ax.set_ylabel("Pacientes")
        ax.grid(axis="x", visible=False)
        ax.set_title(f"Regra 1: óbitos nas primeiras {J1} h após a extubação")
        dxc = r1.diagnostico.map(DX).value_counts()
        subtitulo(ax, f"n = {len(r1)} (IRA {dxc.get('IRA', 0)}, Sepse + IRA {dxc.get('Sepse + IRA', 0)}); "
                      f"{(r1.horas_ate_morte <= 1).sum()} na 1ª hora; {(r1.horas_ate_morte < 0).sum()} antes do T=0.")
        salva(fig, F, "02_hist_obito_precoce_regra1.png")

    # 3 e 4. Regra 2
    r2 = P[P.status_coorte == "excluido_regra2_desfecho_tardio"]
    if len(r2):
        rng = np.random.default_rng(7)
        fig, ax = plt.subplots(figsize=(9.5, 4.8))
        for y, (lab, s, col) in enumerate([("Óbito", r2.horas_ate_morte.dropna() / 24, BLUE),
                                           ("Reintubação", r2.horas_ate_reintubacao.dropna() / 24, ORANGE)]):
            fora = (s > J / 24).values
            jit = rng.uniform(-0.17, 0.17, len(s))
            ax.scatter(s[fora], y + jit[fora], s=34, color=col, edgecolor=SURF, linewidth=1, zorder=3)
            ax.scatter(s[~fora], y + jit[~fora], s=34, facecolor=SURF, edgecolor=col, linewidth=1.4, zorder=3)
            if fora.sum():
                boxplot_h(ax, s[fora], positions=[y], widths=0.5, showfliers=False,
                           patch_artist=True, medianprops=dict(color=INK, lw=1.6),
                           boxprops=dict(facecolor="none", edgecolor=INK2, lw=1),
                           whiskerprops=dict(color=INK2, lw=1), capprops=dict(color=INK2, lw=1))
                ax.text(s[fora].max() * 1.05, y, f"{fora.sum()} >{J // 24} d · mediana {s[fora].median():.1f} d",
                        va="center", fontsize=9, color=INK2)
        ax.axvline(J / 24, color=INK2, lw=1.2, ls=(0, (4, 3)))
        ax.text(J / 24 * 1.02, 1.42, f"limite {J // 24} dias ({J} h)", color=INK2, fontsize=9)
        ax.set_xscale("log")
        ax.set_xlim(right=400)
        ax.set_xticks([1, 2, 5, 10, 20, 50, 100], ["1", "2", "5", "10", "20", "50", "100"])
        ax.set_yticks([0, 1], ["Óbito", "Reintubação"])
        ax.set_ylim(-0.6, 1.6)
        ax.set_xlabel("Dias entre extubação (T=0) e o desfecho (escala log)")
        ax.grid(axis="y", visible=False)
        ax.set_title("Regra 2: tempo até o desfecho dos pacientes excluídos")
        subtitulo(ax, f"n = {len(r2)}. Cheios = desfecho além do limite; vazados = desfecho dentro do limite "
                      f"de quem saiu pelo outro desfecho.")
        salva(fig, F, "03_tempo_desfecho_regra2.png")

        mot = r2.motivo_regra2.value_counts().sort_values()
        fig, ax = plt.subplots(figsize=(9, 3.8))
        ax.barh(range(len(mot)), mot.values, height=0.55, edgecolor=SURF, linewidth=2,
                color=[YELLOW if "após reintubação" in m else ORANGE for m in mot.index])
        for i, v in enumerate(mot.values):
            ax.text(v + 0.6, i, str(v), va="center", fontweight="bold")
        ax.set_yticks(range(len(mot)), mot.index)
        ax.set_xlabel("Pacientes")
        ax.grid(axis="y", visible=False)
        ax.set_title("Regra 2: por que cada paciente saiu")
        subtitulo(ax, f"Amarelo: reintubados dentro de {J} h, excluídos porque o óbito veio depois.")
        salva(fig, F, "04_motivos_regra2.png")

    # 5. Volume de linhas
    fig, ax = plt.subplots(figsize=(9, 5))
    x, w = np.arange(2), 0.26
    trios = [(-w, [vol.resultados_original, vol.eventos_original], GRAY, f"Base original ({n0} pac.)"),
             (0, [vol.resultados_coorte_antes_corte, vol.eventos_coorte_antes_corte], ORANGE,
              f"Coorte, antes do corte ({nf} pac.)"),
             (w, [vol.resultados_final, vol.eventos_final], BLUE, f"Coorte, após corte ≤{J} h")]
    for off, vals, col, lab in trios:
        ax.bar(x + off, vals, width=w - 0.02, color=col, label=lab, edgecolor=SURF, linewidth=2)
        for xi, v in zip(x + off, vals):
            ax.text(xi, v * 1.01, f"{v / 1e3:,.0f} mil".replace(",", "."), ha="center", va="bottom",
                    fontsize=8.5, color=INK2)
    for i in range(2):
        a, d = trios[1][1][i], trios[2][1][i]
        ax.text(x[i] + w / 2, max(trios[0][1][i], a) * 1.12, f"corte remove {(a - d) / a:.1%}",
                ha="center", fontsize=9.5, fontweight="bold")
    ax.set_xticks(x, ["resultados_pacientes", "time_events"])
    ax.set_ylabel("Linhas (registros)")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1e6:.2f} mi"))
    ax.set_ylim(0, vol.resultados_original * 1.25)
    ax.grid(axis="x", visible=False)
    ax.legend(frameon=False, loc="upper right", fontsize=9)
    ax.set_title(f"Regra 3: volume de dados antes e depois da janela de {J} h")
    subtitulo(ax, f"Linhas pré-extubação (delta negativo) são mantidas; só o que passa de +{J} h sai.")
    salva(fig, F, "05_volume_linhas_regra3.png")

    # 6. Perfil da coorte final
    ordem = ["Sobrevivente sem reintubação", "Reintubado", "Óbito", "Reintubado e óbito"]
    cor = dict(zip(ordem, [BLUE, ORANGE, AQUA, YELLOW]))
    tab = pd.crosstab(pac.diagnostico.map(DX), pac.desfecho_5dias).reindex(columns=ordem, fill_value=0)
    tab.loc["Total"] = tab.sum()
    tab = tab.loc[[i for i in ["Total", "Sepse + IRA", "IRA"] if i in tab.index]]
    pct = tab.div(tab.sum(axis=1), axis=0) * 100
    fig, ax = plt.subplots(figsize=(10, 4.6))
    left = np.zeros(len(tab))
    for c in ordem:
        ax.barh(range(len(tab)), pct[c], left=left, color=cor[c], height=0.55,
                edgecolor=SURF, linewidth=2, label=c)
        for i, (p, n) in enumerate(zip(pct[c], tab[c])):
            if p >= 4.5:
                ax.text(left[i] + p / 2, i, f"{n}\n{p:.0f}%", ha="center", va="center", fontsize=8.5,
                        color="white" if c in ordem[:2] else INK)
        left += pct[c].values
    ax.set_yticks(range(len(tab)), [f"{i}  (n={n})" for i, n in zip(tab.index, tab.sum(axis=1))])
    ax.set_xlim(0, 100)
    ax.set_xlabel("% dos pacientes")
    ax.grid(axis="y", visible=False)
    ax.legend(frameon=False, ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.2), fontsize=9)
    ax.set_title(f"Coorte final: desfecho em até {J} h após a extubação")
    pequenos = ", ".join(f"{i} {tab.loc[i, 'Reintubado e óbito']}" for i in tab.index if i != "Total")
    subtitulo(ax, f"Reintubado e óbito (fatia estreita): {pequenos}.")
    salva(fig, F, "06_perfil_coorte_final.png")

    # 7. Tempo até reintubação
    ri = pac.horas_ate_reintubacao[pac.reintubacao_evento_5dias == 1]
    if len(ri):
        fig, ax = plt.subplots(figsize=(9, 4.8))
        ax.hist(ri, bins=np.arange(0, J + 1, 6), color=ORANGE, edgecolor=SURF, linewidth=2)
        for lim in (48, 72):
            ax.axvline(lim, color=INK2, lw=1.2, ls=(0, (4, 3)))
            ax.text(lim + 1, ax.get_ylim()[1] * 0.92, f"{lim} h: {(ri <= lim).sum()} ({(ri <= lim).mean():.0%})",
                    color=INK2, fontsize=9)
        ax.set_xticks(range(0, J + 1, 12))
        ax.set_xlabel("Horas entre extubação (T=0) e reintubação")
        ax.set_ylabel("Pacientes")
        ax.grid(axis="x", visible=False)
        ax.set_title("Coorte final: quando ocorre a reintubação")
        subtitulo(ax, f"n = {len(ri)} reintubados em ≤{J} h; {(ri <= 1).sum()} em ≤1 h (possível artefato de registro).")
        salva(fig, F, "07_tempo_reintubacao_coorte.png")

    # 8. Excluídos: composição e mortalidade (dois painéis, mesma ordem de grupos)
    grupos = [g for g in GRUPOS.values() if g in set(D.grupo)][::-1]
    agg = D.groupby("grupo").agg(n=("sepse", "size"), sepse=("sepse", "mean"), morte=("morte", "mean")).loc[grupos]
    fig, axs = plt.subplots(1, 2, figsize=(11, 4.6), sharey=True)
    for ax, col, tit in [(axs[0], "sepse", "% Sepse + IRA"), (axs[1], "morte", "% óbito hospitalar")]:
        cores = [BLUE if g == "Coorte final" else ORANGE for g in grupos]
        ax.barh(range(len(grupos)), agg[col] * 100, color=cores, height=0.55, edgecolor=SURF, linewidth=2)
        for i, v in enumerate(agg[col] * 100):
            ax.text(v + 1.5, i, f"{v:.0f}%", va="center", fontsize=9.5, fontweight="bold")
        ax.set_xlim(0, 115)
        ax.set_xticks([0, 25, 50, 75, 100])
        ax.grid(axis="y", visible=False)
        ax.set_title(tit, fontsize=11.5, pad=8)
    axs[0].set_yticks(range(len(grupos)), [f"{g}  (n={agg.loc[g, 'n']})" for g in grupos])
    fig.suptitle("Quem saiu da coorte: diagnóstico e mortalidade por motivo de exclusão",
                 x=0.01, ha="left", fontweight="bold", fontsize=13)
    fig.tight_layout()
    fig.savefig(os.path.join(F, "08_excluidos_diagnostico_mortalidade.png"), dpi=160)
    plt.close(fig)

    # 9. Excluídos: vitais nas primeiras 24 h de UTI (small multiples)
    vs = [(v, r) for v, r in VITAIS_STATS if v in D][:6]
    gr = grupos[::-1]
    fig, axs = plt.subplots(2, 3, figsize=(13, 7.5))
    for ax, (v, rot) in zip(axs.flat, vs):
        dados = [D.loc[D.grupo == g, v].dropna() for g in gr]
        bp = boxplot_h(ax, dados, widths=0.55, showfliers=False, patch_artist=True,
                        medianprops=dict(color=INK, lw=1.6), whiskerprops=dict(color=INK2),
                        capprops=dict(color=INK2))
        for patch, g in zip(bp["boxes"], gr):
            patch.set(facecolor=BLUE if g == "Coorte final" else ORANGE, alpha=0.85, edgecolor=SURF, linewidth=1.5)
        ax.set_yticks(range(1, len(gr) + 1), gr if ax in axs[:, 0] else [""] * len(gr))
        ax.invert_yaxis()
        ax.grid(axis="y", visible=False)
        ax.set_title(rot, fontsize=11, pad=6)
    for ax in list(axs.flat)[len(vs):]:
        ax.axis("off")
    fig.suptitle("Quem saiu da coorte: sinais vitais e exames nas primeiras 24 h de UTI "
                 "(mediana por paciente; caixa = IQR)", x=0.01, ha="left", fontweight="bold", fontsize=13)
    fig.tight_layout()
    fig.savefig(os.path.join(F, "09_excluidos_vitais_24h_uti.png"), dpi=160)
    plt.close(fig)
    print(f"\nGráficos salvos em {F}: {sorted(os.listdir(F))}")


# =============================================================================
# Exportação
# =============================================================================
def exportar(P, coorte, res_f, ev_f, log, vol, pasta, suf):
    cols = ["diagnostico", "morte", "reintubacao", "n_extubacoes", "n_intubacoes_pre_T0",
            "n_intubacoes_total", "T0_extubacao", "data_morte", "data_reintubacao",
            "data_alta_hospitalar", "horas_ate_morte", "horas_ate_reintubacao", "horas_ate_alta",
            "horas_VM_ate_T0", "desfecho_5dias", "reintubacao_evento_5dias", "morte_5dias"]
    out = P.loc[coorte, cols].reset_index()
    out.insert(out.columns.get_loc("T0_extubacao") + 1, "Tempo_desde_Extubacao", 0.0)
    out.to_csv(os.path.join(pasta, f"pacientes{suf}.csv"), index=False)
    res_f.to_csv(os.path.join(pasta, f"resultados_pacientes{suf}.csv"), index=False)
    ev_f.to_csv(os.path.join(pasta, f"time_events{suf}.csv"), index=False)
    P.reset_index()[["hadm_id", "diagnostico", "morte", "reintubacao", "n_extubacoes",
                     "n_intubacoes_pre_T0", "n_intubacoes_total", "T0_extubacao",
                     "horas_ate_morte", "horas_ate_reintubacao", "status_coorte",
                     "motivo_regra2", "desfecho_5dias"]] \
        .to_csv(os.path.join(pasta, f"auditoria_pacientes{suf}.csv"), index=False)
    pd.DataFrame(log).to_csv(os.path.join(pasta, f"log_exclusoes{suf}.csv"), index=False)
    vol.to_csv(os.path.join(pasta, f"volume_linhas{suf}.csv"))


def checagens(P, coorte, res_f, ev_f, cfg):
    J = cfg["janela_desfecho_h"]
    c = P.loc[coorte]
    assert (c.horas_ate_morte <= cfg["janela_obito_precoce_h"]).sum() == 0
    assert res_f.Tempo_desde_Extubacao.max() <= J and ev_f.Tempo_desde_Extubacao.max() <= J
    assert set(res_f.hadm_id) <= set(coorte) and set(ev_f.hadm_id) <= set(coorte)
    if cfg["excluir_multiplas_intubacoes"] is True:
        assert (c.n_intubacoes_pre_T0 > 1).sum() == 0
    if not cfg["regra2_apenas_primeiro_desfecho"]:
        assert ((c.horas_ate_morte > J) | (c.horas_ate_reintubacao > J)).sum() == 0
    print("Checagens OK.")


# =============================================================================
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dados", default=PADRAO_DADOS,
                    help=f"pasta com os 3 arquivos de entrada (padrão: {PADRAO_DADOS})")
    ap.add_argument("--saida", default=None, help=f"pasta de saída (padrão: {PADRAO_SAIDA})")
    args = ap.parse_args()
    cfg = dict(CONFIG)
    pasta = args.saida or PADRAO_SAIDA
    os.makedirs(pasta, exist_ok=True)

    pac, ev, res = carregar(args.dados)
    print("\nExclusões:")
    P, t0 = definir_eventos(pac, ev, cfg)
    P, coorte, log = aplicar_exclusoes(P, cfg)
    res, res_f, ev_f, vol = delta_e_truncamento(res, ev, t0, coorte, log, cfg)
    checagens(P, coorte, res_f, ev_f, cfg)

    print("\nCoorte final por diagnóstico e desfecho em 5 dias:")
    print(P.loc[coorte].groupby(["diagnostico", "desfecho_5dias"]).size().to_string())

    D = estatisticas_excluidos(P, res, pasta)
    graficos(P, D, vol, cfg, pasta)
    exportar(P, coorte, res_f, ev_f, log, vol, pasta, cfg["sufixo"])
    print(f"\nPronto. Tudo salvo em {os.path.abspath(pasta)}")


if __name__ == "__main__":
    main()