"""
단건(1 계약) end-to-end 테스트
INFRC_IDNO 1건만 로딩하여 cf_module 파이프라인을 실행하고,
기존 모듈(timing3, qx_rate_table, lapse, skew)과 결과를 비교한다.

Usage:
    python test_single_contract.py              # 기본값: 8833
    python test_single_contract.py --idno 1234  # 특정 계약
"""

import argparse
import time
import numpy as np

# ── 0. 인자 파싱 ──
parser = argparse.ArgumentParser(description="단건 계약 테스트")
parser.add_argument("--idno", type=int, default=8833, help="INFRC_IDNO (기본값: 8833)")
parser.add_argument("--pricing", action="store_true", help="Pricing 모드 (예정기초율 사용)")
args = parser.parse_args()

# ── 0-1. DEBUG 모드 활성화 ──
from cf_module.utils.logger import enable_debug
enable_debug()

# ── 1. cf_module 파이프라인 실행 ──

from cf_module.config import CFConfig, DBConfig, RunsetParams, ProjectionConfig, BatchConfig, ScenarioConfig, OutputConfig
from cf_module.io.reader import DataReader
from cf_module.data.model_point import load_model_points
from cf_module.data.assumptions import AssumptionLoader
from cf_module.projection.projector import run_projection, result_to_summary_df

DB_PATH = r"C:\Users\thrus\Downloads\VSOLN2\VSOLN2.vdb"

run_mode = "pricing" if args.pricing else "valuation"

config = CFConfig(
    db=DBConfig(db_type="sqlite", path=DB_PATH),
    runset=RunsetParams(infrc_seq=1, infrc_idno=args.idno, clos_ym="202309", assm_ym="202306"),
    projection=ProjectionConfig(time_step="monthly", base_date="202309"),
    batch=BatchConfig(chunk_size=100_000),
    scenario=ScenarioConfig(),
    output=OutputConfig(output_dir="./output"),
    run_targets=["ifrs17"],
    run_mode=run_mode,
    debug=True,
)

print("=" * 60)
print(f"cf_module 단건 테스트: INFRC_IDNO={args.idno}, mode={run_mode}")
print("=" * 60)

# Reader 생성 + 쿼리 로딩
reader = DataReader(config.db)
reader.load_queries(config.queries_path)

# ── MP 로딩: DB에서 1건만 조회 (II_INFRC_SINGLE) ──
t0 = time.time()
print("\n[1] MP 로딩 중...")

mp = load_model_points(
    reader, config,
    query_name="II_INFRC_SINGLE",
    params=config.runset.query_params_single,
)
print(f"    로딩 완료: {mp.n_points}건 ({time.time()-t0:.1f}s)")

print(f"\n[MP 정보]")
print(f"  mp_id       : {mp.mp_ids}")
print(f"  product_cd  : {mp.product_cd}")
print(f"  sex_cd      : {mp.sex_cd}")
print(f"  age_at_entry: {mp.age_at_entry}")
print(f"  bterm       : {mp.bterm}")
print(f"  pterm       : {mp.pterm}")
print(f"  premium     : {mp.premium}")
print(f"  sum_assured : {mp.sum_assured}")

# ── 가정 로딩 ──
print("\n[2] 가정 데이터 로딩 중...")
t1 = time.time()
loader = AssumptionLoader(reader, config)
assumptions = loader.load_all(
    params=config.runset.query_params_with_assm,
    mp_ids=mp.mp_ids,
    mp=mp,
)
print(f"    로딩 완료 ({time.time()-t1:.1f}s)")
print(f"  mortality: rsk_rt_cd={assumptions.mortality.rsk_rt_cd}")
print(f"  mortality: chr_cd={assumptions.mortality.chr_cd}")
print(f"  mortality: mm_trf={assumptions.mortality.mm_trf_way_cd}")
print(f"  mortality: raw_val shape={assumptions.mortality.raw_val.shape if assumptions.mortality.raw_val is not None else None}")
print(f"  lapse: raw_data shape={assumptions.lapse.raw_data.shape if assumptions.lapse.raw_data is not None else None}")
print(f"  skew: raw_data shape={assumptions.skew.raw_data.shape if assumptions.skew.raw_data is not None else None}")
print(f"  beprd: raw_data shape={assumptions.beprd_raw_data.shape if assumptions.beprd_raw_data is not None else None}")

if args.pricing:
    print(f"\n[Pricing REVI_YM 검증]")
    print(f"  CTR_YM: {mp.ctr_ym[0]}")
    print(f"  mortality REVI_YM: {assumptions.mortality.revi_ym}")
    print(f"\n[예정기초율 (Pricing)]")
    ei = assumptions.expected_interest
    if ei is not None:
        flat = ei.get_flat_rate()
        print(f"  예정이율: rates={ei.rates}, change_years={ei.change_years}")
        print(f"  단일이율: {flat}" if flat is not None else f"  3단계 이율")
    else:
        print(f"  예정이율: 없음")
    ee = assumptions.expected_expense
    if ee is not None:
        print(f"  신계약비(영보): {ee.fryy_gprem_acqs_rt:.4f}")
        print(f"  신계약비(가입금액): {ee.fryy_join_amt_acqs_rt:.6f}")
        print(f"  유지비(영보): {ee.inpay_gprem_mnt_rt:.4f}")
        print(f"  유지비(고정금액): {ee.inpay_fxamt_mntexp:.0f}")
        print(f"  납입후유지비(영보): {ee.afpay_gprem_mnt_rt:.4f}")
        print(f"  수금비: {ee.inpay_gprem_colm_rt:.4f}")
        print(f"  손해조사비: {ee.inpay_gprem_loss_svyexp_rt:.4f}")
    else:
        print(f"  예정사업비: 없음")

# ── 프로젝션 실행 ──
print("\n[3] 프로젝션 실행 중...")
t2 = time.time()
result = run_projection(mp, assumptions, config, reader=reader)
print(f"    프로젝션 완료 ({time.time()-t2:.1f}s)")

# ── 결과 요약 ──
print("\n" + "=" * 60)

if not args.pricing:
    # ── Valuation 모드: 중간결과 상세 출력 ──
    print("cf_module 결과 요약 (Valuation)")
    print("=" * 60)

    dec = result.decrement
    print(f"\n[사망률 qx_monthly]")
    print(f"  shape: {dec.qx_monthly.shape}")
    print(f"  처음 12개월: {dec.qx_monthly[0, :12]}")
    print(f"  sum: {np.sum(dec.qx_monthly[0, :]):.8f}")

    if dec.qx_be_by_risk is not None:
        print(f"\n[위험률코드별 qx_be_by_risk]")
        print(f"  shape: {dec.qx_be_by_risk.shape}")
        print(f"  rsk_rt_cd: {dec.rsk_rt_cd}")
        for j in range(min(5, dec.qx_be_by_risk.shape[1])):
            vals = dec.qx_be_by_risk[:12, j]
            print(f"  risk[{j}] 처음 12개월: {vals}")

    print(f"\n[해약률 wx_monthly]")
    print(f"  shape: {dec.wx_monthly.shape}")
    print(f"  처음 12개월: {dec.wx_monthly[0, :12]}")

    if dec.skew is not None:
        print(f"\n[스큐]")
        print(f"  shape: {dec.skew.shape}")
        print(f"  처음 24개월: {dec.skew[:24]}")

    print(f"\n[생존확률 tpx]")
    print(f"  처음 12개월: {dec.tpx[0, :12]}")

    # 타이밍 정보
    timing = result.timing
    print(f"\n[타이밍]")
    print(f"  n_steps: {timing.n_steps}")
    print(f"  duration_years[0,:12]: {timing.duration_years[0, :12]}")
    print(f"  duration_months[0,:12]: {timing.duration_months[0, :12]}")
    print(f"  age[0,:12]: {timing.age[0, :12]}")

    # ── PAY 결과 출력 ──
    if dec.pay_tpx is not None:
        print(f"\n[PAY 결과]")
        print(f"  pay_tpx[0,:12]: {dec.pay_tpx[0, :12]}")
        if dec.pay_d_rsvamt is not None:
            print(f"  pay_d_rsvamt[0,:5]: {dec.pay_d_rsvamt[0, :5]}")
        if dec.pay_d_bnft is not None:
            print(f"  pay_d_bnft[0,:5]: {dec.pay_d_bnft[0, :5]}")
        if dec.pay_d_pyexsp is not None:
            print(f"  pay_d_pyexsp[0,:5]: {dec.pay_d_pyexsp[0, :5]}")
        if dec.pay_qx_monthly is not None:
            print(f"  pay_qx_monthly[0,:5]: {dec.pay_qx_monthly[0, :5]}")
        if dec.pay_dx_monthly is not None:
            print(f"  pay_dx_monthly[0,:5]: {dec.pay_dx_monthly[0, :5]}")
    else:
        print(f"\n[PAY 결과] pay_tpx 미생성")

    # ── 760397 기대값 비교 ──
    if args.idno == 760397:
        print(f"\n[PROJ_O2.vdb 기대값 비교 (760397)]")
        expected = {
            "PAY_TRME_t1": 0.9925024849,
            "PAY_TRMNAT_RT_t1": 0.0063239731,
            "PYEXSP_DRPO_RSKRT_t1": 0.0010430155,
        }

        tol = 1e-6
        if dec.pay_tpx is not None:
            pay_tpx_t1 = dec.pay_tpx[0, 0]
            diff = abs(pay_tpx_t1 - expected["PAY_TRME_t1"])
            status = "PASS" if diff < tol else "FAIL"
            print(f"  [{status}] PAY_TRME(t=1): {pay_tpx_t1:.10f} (기대: {expected['PAY_TRME_t1']:.10f}, diff={diff:.2e})")

        if dec.pay_qx_monthly is not None:
            pay_trmnat_rt = dec.pay_qx_monthly[0, 0]
            diff = abs(pay_trmnat_rt - expected["PAY_TRMNAT_RT_t1"])
            status = "PASS" if diff < tol else "FAIL"
            print(f"  [{status}] PAY_TRMNAT_RT(t=1): {pay_trmnat_rt:.10f} (기대: {expected['PAY_TRMNAT_RT_t1']:.10f}, diff={diff:.2e})")

        if dec.pay_d_pyexsp is not None:
            pyexsp_rt = dec.pay_d_pyexsp[0, 0]  # tpx_bot[0,0]=1이므로 rate와 동일
            diff = abs(pyexsp_rt - expected["PYEXSP_DRPO_RSKRT_t1"])
            status = "PASS" if diff < tol else "FAIL"
            print(f"  [{status}] PYEXSP_DRPO_RSKRT(t=1): {pyexsp_rt:.10f} (기대: {expected['PYEXSP_DRPO_RSKRT_t1']:.10f}, diff={diff:.2e})")

    # 요약 DF
    summary = result_to_summary_df(result)
    print(f"\n[요약 DF]")
    print(summary)

# ── Pricing 결과 ──
if args.pricing and result.pricing_result is not None:
    pr = result.pricing_result
    ct = pr.commutation
    n = ct.n
    calc_sa = pr.crit_join_amt or mp.sum_assured[0]

    print(f"Pricing 결과 (기준SA={calc_sa:,.0f} 기준)")
    print(f"{'=' * 60}")
    print(f"  가입연령: {ct.x}세")
    print(f"  보장기간: {ct.n}년")
    print(f"  납입기간: {ct.m}년")
    print(f"  예정이율: {ct.i*100:.2f}%")
    print(f"  pay_freq: {pr.pay_freq} (연간 납입횟수)")
    print(f"  만기급부: {'있음 (양로보험)' if pr.has_maturity else '없음 (정기보험)'}")

    if pr.crit_join_amt is not None:
        print(f"\n  [기준가입금액]")
        print(f"    CRIT_JOIN_AMT: {pr.crit_join_amt:,.0f}")
        print(f"    실제SA: {mp.sum_assured[0]:,.0f}")
        print(f"    배수(multiplier): {pr.multiplier:,.0f}")

    # ── 순보험료 P (기준SA 기준) ──
    print(f"\n  [순보험료 P -기준SA 기준]")
    print(f"    Ax: {pr.Ax:.10f}")
    print(f"    ax_due(x:m): {pr.ax_due:.10f}")
    print(f"    ax_due_woolhouse: {pr.ax_due_monthly:.10f}")
    print(f"    P (1회납, raw): {pr.net_premium_monthly:.4f}")
    if pr.net_premium_monthly_rounded is not None:
        print(f"    P (1회납, 라운드): {pr.net_premium_monthly_rounded:,d}")
    print(f"    P (연납): {pr.net_premium_annual:.4f}")

    # ── 영업보험료 G (기준SA 기준) ──
    if pr.gross_premium_annual is not None:
        print(f"\n  [영업보험료 G -기준SA 기준]")
        print(f"    t_acq: {pr.acq_amort_period}")
        print(f"    G (1회납, raw): {pr.gross_premium_monthly:.4f}")
        if pr.gross_premium_monthly_rounded is not None:
            print(f"    G (1회납, 라운드): {pr.gross_premium_monthly_rounded:,d}")
        print(f"    G (연납): {pr.gross_premium_annual:.4f}")
        print(f"    부가비율(G/P): {pr.loading_ratio:.4f}")

    # ── PV 요약 테이블 (기준SA 기준) ──
    if pr.gross_premium_annual is not None and assumptions.expected_expense is not None:
        import pandas as pd
        from cf_module.calc.commutation import _woolhouse_annuity

        ee_pv = assumptions.expected_expense
        ax_m = _woolhouse_annuity(ct, ct.m, pr.pay_freq)
        t_acq_pv = pr.acq_amort_period or ct.m
        ax_t = _woolhouse_annuity(ct, t_acq_pv, pr.pay_freq)
        ax_n = _woolhouse_annuity(ct, ct.n, pr.pay_freq)
        G = pr.gross_premium_annual
        SA = calc_sa

        a = ee_pv.fryy_gprem_acqs_rt
        a_sa = ee_pv.fryy_join_amt_acqs_rt
        b_acq = ee_pv.inpay_gprem_acqs_rt
        b_mnt = ee_pv.inpay_gprem_mnt_rt
        g_rt = ee_pv.inpay_gprem_colm_rt if ee_pv.inpay_gprem_colm_rt != 0.0 else 0.02
        d_rt = ee_pv.inpay_gprem_loss_svyexp_rt
        K_val = ee_pv.inpay_fxamt_mntexp
        b_af = ee_pv.afpay_gprem_mnt_rt

        # 확정연금 ä_{m|} = 1+v+...+v^{m-1}
        v_disc = 1.0 / (1.0 + ct.i)
        a_certain = sum(v_disc ** t for t in range(ct.m)) if ct.m > 0 else 1.0
        alpha_rate = a / a_certain if a_certain > 1e-12 else 0.0

        pv_inc = G * ax_m
        pv_ben = pr.Ax * SA
        pv_a_g = alpha_rate * G * ax_m  # α/ä_{m|} × G × ä^(12)
        pv_a_sa = a_sa * SA
        pv_b_acq = b_acq * G * ax_m
        pv_b_mnt = b_mnt * G * ax_m
        pv_colm = g_rt * G * ax_m
        pv_loss = d_rt * G * ax_m
        pv_fix = K_val * ax_m
        pv_af = b_af * G * (ax_n - ax_m)
        pv_exp = pv_ben + pv_a_g + pv_a_sa + pv_b_acq + pv_b_mnt + pv_colm + pv_loss + pv_fix + pv_af

        pv_rows = [
            ["[수입]", "", "", "", ""],
            ["영업보험료", "-", f"{G:.2f}", f"{ax_m:.6f}", f"{pv_inc:.2f}"],
            ["", "", "", "", ""],
            ["[지출]", "", "", "", ""],
            ["급부(Ax*SA)", f"{pr.Ax:.6f}", f"{SA:,.0f}", "-", f"{pv_ben:.2f}"],
            [f"신계약비(α/ä_{{{ct.m}|}})", f"{alpha_rate:.6f}", f"{G:.2f}", f"{ax_m:.6f}", f"{pv_a_g:.2f}"],
            ["신계약비(SA)", f"{a_sa:.6f}", f"{SA:,.0f}", "-", f"{pv_a_sa:.2f}"],
            ["납입중신계약비", f"{b_acq:.4f}", f"{G:.2f}", f"{ax_m:.6f}", f"{pv_b_acq:.2f}"],
            ["유지비(영보)", f"{b_mnt:.4f}", f"{G:.2f}", f"{ax_m:.6f}", f"{pv_b_mnt:.2f}"],
            ["수금비", f"{g_rt:.4f}", f"{G:.2f}", f"{ax_m:.6f}", f"{pv_colm:.2f}"],
            ["손해조사비", f"{d_rt:.4f}", f"{G:.2f}", f"{ax_m:.6f}", f"{pv_loss:.2f}"],
            ["고정유지비", f"{K_val:.0f}", "-", f"{ax_m:.6f}", f"{pv_fix:.2f}"],
            ["납입후유지비", f"{b_af:.4f}", f"{G:.2f}", f"{ax_n - ax_m:.6f}", f"{pv_af:.2f}"],
            ["", "", "", "", ""],
            ["지출합계", "", "", "", f"{pv_exp:.2f}"],
            ["차이(수입-지출)", "", "", "", f"{pv_inc - pv_exp:.6f}"],
        ]

        pv_df = pd.DataFrame(pv_rows, columns=["항목", "비율", "금액", "연금현가", "PV"])
        print(f"\n  [PV 요약 (기준SA={calc_sa:,.0f})]")
        print(pv_df.to_string(index=False))

    # ── 준비금 V(t) (기준SA 기준) ──
    V = pr.reserve_by_year
    V_r = pr.reserve_by_year_rounded
    print(f"\n  [준비금 V(t) -기준SA 기준]")
    show_first = min(6, n + 1)
    show_last = min(3, n + 1 - show_first)
    for t in range(show_first):
        if V_r is not None:
            print(f"    V({t}) = {V[t]:,.4f}  → 라운드: {int(V_r[t]):,d}")
        else:
            print(f"    V({t}) = {V[t]:,.4f}")
    if n + 1 > show_first + show_last:
        print(f"    ...")
    for t in range(max(show_first, n + 1 - show_last), n + 1):
        if V_r is not None:
            print(f"    V({t}) = {V[t]:,.4f}  → 라운드: {int(V_r[t]):,d}")
        else:
            print(f"    V({t}) = {V[t]:,.4f}")

    # ── 실제SA 적용 (multiplier) ──
    if pr.multiplier is not None and pr.multiplier != 1.0:
        m = pr.multiplier
        print(f"\n  [실제SA 적용 (× {m:.0f})]")
        if pr.net_premium_monthly_rounded is not None:
            p_actual = pr.net_premium_monthly_rounded * m
            print(f"    P (1회납): {pr.net_premium_monthly_rounded} × {m:.0f} = {p_actual:,.0f}")
            print(f"    P (연납): {p_actual * pr.pay_freq:,.0f}")
        if pr.gross_premium_monthly_rounded is not None:
            g_actual = pr.gross_premium_monthly_rounded * m
            print(f"    G (1회납): {pr.gross_premium_monthly_rounded} × {m:.0f} = {g_actual:,.0f}")
            print(f"    G (연납): {g_actual * pr.pay_freq:,.0f}")
        if V_r is not None:
            for t in range(show_first):
                if t > 0 and int(V_r[t]) != 0:
                    print(f"    V({t}): {int(V_r[t])} × {m:.0f} = {int(V_r[t]) * m:,.0f}")

    print(f"\n  [debug CSV]")
    print(f"    00_model_point.csv     : MP 정보")
    print(f"    08_commutation.csv     : 계산기수 (Dx, Nx, Cx, Mx, V)")
    print(f"    09_pricing_summary.csv : P, V 요약")
    print(f"    10_gross_premium.csv   : 영업보험료 산출과정")
    print(f"    경로: ./output/debug/")

print(f"\n총 소요시간: {time.time()-t0:.1f}s")
print("=" * 60)
