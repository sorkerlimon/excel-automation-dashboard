from pathlib import Path
import os
import pandas as pd
import time

starttime = time.time()

path = Path(__file__).parent.absolute()
name = f"{path}\\Combine\\super_merge.csv"

df = pd.read_csv(name)

df = df.replace('', 0.0)
df.drop(df.columns[0], axis=1, inplace=True)
df['Date'] = pd.to_datetime(df['Date'])
df.iloc[1:, 6:] = df.iloc[1:, 6:].astype(float)


df = df.sort_values(['Date','NE Name','Cell Name']).groupby(['Date','NE Name','Cell Name']).agg(
    col_4 = ('Total Traffic (Mbit)_LRNO (MB)', lambda x: x.sum() / 8 / 1024/ 1024),
    col_5 = ('DL Traffic (Mbit)_LRNO (MB)', lambda x: x.sum() / 8 / 1024/ 1024),
    col_6 = ('UL Traffic (Mbit)_LRNO (MB)', lambda x: x.sum() / 8 / 1024/ 1024),
    col_7a = ('User Throughput in DownLink (Mbps)_N_LRNO (number)', lambda x: x.sum()),
    col_7b = ('User Throughput in DownLink (Mbps)_D_LRNO (number)', lambda x: x.sum()),
    col_8a = ('User Throughput in UpLink (Mbps)_N_LRNO (number)', lambda x: x.sum()),
    col_8b = ('User Throughput in UpLink (Mbps)_D_LRNO (number)', lambda x: x.sum()),
    col_9a = ('DL PDCP Throughput (Mbit/s)_N_LRNO (Mbit/s)', lambda x: x.sum()),
    col_9b = ('DL PDCP Throughput (Mbit/s)_D_LRNO (Mbit/s)', lambda x: x.sum()),

    col_10a = ('UL PDCP Throughput (Mbit/s)_N_LRNO (Mbit/s)', lambda x: x.sum()),
    col_10b = ('UL PDCP Throughput (Mbit/s)_D_LRNO (number)', lambda x: x.sum()),
    col_11 = ('Mean Number of Users in a Cell_LRNO (number)', lambda x: x.mean()),
    col_12 = ('Maximum Number of Users(Cell)_LRNO (number)', lambda x: x.max()),
    col_13a = ('Call  Setup Success Rate_N_LRNO (number)', lambda x: x.sum()),
    col_13b = ('Call  Setup Success Rate_D_LRNO (number)', lambda x: x.sum()),
    col_14a = ('PS E UTRAN RRC Setup successful Ratio_N_LRNO (number)', lambda x: x.sum()),
    col_14b = ('PS E UTRAN RRC Setup successful Ratio_D_LRNO (number)', lambda x: x.sum()),
    col_15a = ('PS E UTRAN RAB Setup Success Rate_N_LRNO (number)', lambda x: x.sum()),
    col_15b = ('PS E UTRAN RAB Setup Success Rate_D_LRNO (number)', lambda x: x.sum()),
    col_16a = ('S1 Sig Setup Success Rate_N_LRNO (number)', lambda x: x.sum()),
    col_16b = ('S1 Sig Setup Success Rate_D_LRNO (number)', lambda x: x.sum()),
    col_17a = ('SecurityMod_N_LRNO (number)', lambda x: x.sum()),
    col_17b = ('SecurityMod_D_LRNO (number)', lambda x: x.sum()),
    col_18a = ('PS Drop Call Rate_N_LRNO (number)', lambda x: x.sum()),
    col_18b = ('PS Drop Call Rate_D_LRNO (number)', lambda x: x.sum()),
    col_19 = ('L.CSFB.E2W (None)', lambda x: x.sum()),
    col_20 = ('L.CSFB.E2G (None)', lambda x: x.sum()),

    col_21a = ('L.CSFB.PrepSucc (None)', lambda x: x.sum()),
    col_21b = ('L.CSFB.PrepAtt (None)', lambda x: x.sum()),
    col_22a = ('Average PRB Utilization (DL)_N_LRNO (number)', lambda x: x.sum()),
    col_22b = ('Average PRB Utilization (DL)_D_LRNO (number)', lambda x: x.sum()),
    col_23a = ('Average PRB Utilization (UL)_N_LRNO (number)', lambda x: x.sum()),
    col_23b = ('Average PRB Utilization (UL)_D_LRNO (number)', lambda x: x.sum()),
    col_24a = ('ERAB_Cong_N_LRNO (number)', lambda x: x.sum()),
    col_24b = ('ERAB_CONG_D_LRNO (number)', lambda x: x.sum()),
    col_25a = ('DL Packet Loss Rate_N_LRNO (number)', lambda x: x.sum()),
    col_25b = ('DL Packet Loss Rate_D_LRNO (number)', lambda x: x.sum()),
    col_26a = ('UL Packet Loss Rate_N_LRNO (number)', lambda x: x.sum()),
    col_26b = ('UL Packet Loss Rate_D_LRNO (number)', lambda x: x.sum()),
    col_27 = ('CellUnavailTime_LRNO (s)', lambda x: x.sum()),
    col_28a = ('Handover over X2 success Ratio_N_LRNO (number)', lambda x: x.sum()),
    col_28b = ('Handover over X2 success Ratio_D_LRNO (number)', lambda x: x.sum()),
    col_29a = ('Intra Frequency Handover Ratio_N_LRNO (number)', lambda x: x.sum()),
    col_29b = ('Intra Frequency Handover Ratio_D_LRNO (number)', lambda x: x.sum()),
    col_30a = ('Inter RAT Handover out Success Rate (L2W)_N_LRNO (number)', lambda x: x.sum()),
    col_30b = ('Inter RAT Handover out Success Rate (L2W)_D_LRNO (number)', lambda x: x.sum()),
    col_31a = ('Handover In Success Rate_N_LRNO (number)', lambda x: x.sum()),
    col_31b = ('Handover In Success Rate_D_LRNO (number)', lambda x: x.sum()),
    col_32 = ('Total Traffic (Mbit)_LRNO (MB)', lambda x: x.sum()),
    col_33 = ('UL Traffic (Mbit)_LRNO (MB)', lambda x: x.sum()),
    col_34 = ('DL Traffic (Mbit)_LRNO (MB)', lambda x: x.sum()),
    col_35 = ('Call  Setup Success Rate_N_LRNO (number)', lambda x: x.sum()),
    col_36 = ('Call  Setup Success Rate_D_LRNO (number)', lambda x: x.sum()),
    col_37 = ('User Throughput in DownLink (Mbps)_N_LRNO (number)', lambda x: x.sum()),
    col_38 = ('User Throughput in DownLink (Mbps)_D_LRNO (number)', lambda x: x.sum()),
    col_39 = ('User Throughput in UpLink (Mbps)_N_LRNO (number)', lambda x: x.sum()),
    col_40 = ('User Throughput in UpLink (Mbps)_D_LRNO (number)', lambda x: x.sum()),
    col_41 = ('Mean Number of Users in a Cell_LRNO (number)', lambda x: x.mean()),
    col_42 = ('Maximum Number of Users(Cell)_LRNO (number)', lambda x: x.max()),
    col_43 = ('PS Drop Call Rate_N_LRNO (number)', lambda x: x.sum()),
    col_44 = ('PS Drop Call Rate_D_LRNO (number)', lambda x: x.sum()),
    col_45 = ('PS E UTRAN RAB Setup Success Rate_N_LRNO (number)', lambda x: x.sum()),
    col_46 = ('PS E UTRAN RAB Setup Success Rate_D_LRNO (number)', lambda x: x.sum()),
    col_47 = ('PS E UTRAN RRC Setup successful Ratio_N_LRNO (number)', lambda x: x.sum()),
    col_48 = ('PS E UTRAN RRC Setup successful Ratio_D_LRNO (number)', lambda x: x.sum()),
    col_49 = ('DL Packet Loss Rate_N_LRNO (number)', lambda x: x.sum()),
    col_50 = ('DL Packet Loss Rate_D_LRNO (number)', lambda x: x.sum()),
    col_51 = ('UL Packet Loss Rate_N_LRNO (number)', lambda x: x.sum()),
    col_52 = ('UL Packet Loss Rate_D_LRNO (number)', lambda x: x.sum()),
    col_53 = ('Average PRB Utilization (DL)_N_LRNO (number)', lambda x: x.sum()),
    col_54 = ('Average PRB Utilization (DL)_D_LRNO (number)', lambda x: x.sum()),
    col_55 = ('Average PRB Utilization (UL)_N_LRNO (number)', lambda x: x.sum()),
    col_56 = ('Average PRB Utilization (UL)_D_LRNO (number)', lambda x: x.sum()),
    col_57 = ('Handover over X2 success Ratio_N_LRNO (number)', lambda x: x.sum()),
    col_58 = ('Handover over X2 success Ratio_D_LRNO (number)', lambda x: x.sum()),
    col_59 = ('Intra Frequency Handover Ratio_N_LRNO (number)', lambda x: x.sum()),
    col_60 = ('Intra Frequency Handover Ratio_D_LRNO (number)', lambda x: x.sum()),
    col_61 = ('Inter RAT Handover out Success Rate (L2W)_N_LRNO (number)', lambda x: x.sum()),
    col_62 = ('Inter RAT Handover out Success Rate (L2W)_D_LRNO (number)', lambda x: x.sum()),
    col_63 = ('Inter RAT Redirection In Success Rate (L2G)_N_LRNO (number)', lambda x: x.sum()),
    col_64 = ('Inter RAT Redirection In Success Rate (L2W)_N_LRNO (number)', lambda x: x.sum()),
    col_65= ('S1 Sig Setup Success Rate_N_LRNO (number)',lambda x: x.sum()),
    col_66= ('S1 Sig Setup Success Rate_D_LRNO (number)',lambda x: x.sum()),
    col_67= ('DL PDCP Throughput (Mbit/s)_N_LRNO (Mbit/s)',lambda x: x.sum()),
    col_68= ('DL PDCP Throughput (Mbit/s)_D_LRNO (Mbit/s)',lambda x: x.sum()),
    col_69= ('UL PDCP Throughput (Mbit/s)_N_LRNO (Mbit/s)',lambda x: x.sum()),
    col_70= ('UL PDCP Throughput (Mbit/s)_D_LRNO (number)',lambda x: x.sum()),
    col_71= ('L.CSFB.E2W (None)',lambda x: x.sum()),
    col_72= ('L.CSFB.E2G (None)',lambda x: x.sum()),
    col_73= ('L.RRCRedirection.E2W.CSFB (None)',lambda x: x.sum()),
    col_74= ('L.RRCRedirection.E2G.CSFB (None)',lambda x: x.sum()),
    col_75= ('L.CSFB.PrepSucc (None)',lambda x: x.sum()),
    col_76= ('L.CSFB.PrepAtt (None)',lambda x: x.sum()),
    col_77= ('SecurityMod_N_LRNO (number)',lambda x: x.sum()),
    col_78= ('SecurityMod_D_LRNO (number)',lambda x: x.sum()),
    col_79= ('Handover In Success Rate_N_LRNO (number)',lambda x: x.sum()),
    col_80= ('Handover In Success Rate_D_LRNO (number)',lambda x: x.sum()),
    col_81= ('CellUnavailTime_LRNO (s)',lambda x: x.sum()),
    col_82= ('ERAB_Cong_N_LRNO (number)',lambda x: x.sum()),
    col_83= ('ERAB_CONG_D_LRNO (number)',lambda x: x.sum()),
    col_84= ('TA0_LRNO (number)',lambda x: x.sum()),
    col_85= ('TA1_LRNO (number)',lambda x: x.sum()),
    col_86= ('TA2_LRNO (number)',lambda x: x.sum()),
    col_87= ('TA3_LRNO (number)',lambda x: x.sum()),
    col_88= ('TA4_LRNO (number)',lambda x: x.sum()),
    col_89= ('TA5_LRNO (number)',lambda x: x.sum()),
    col_90= ('TA6_LRNO (number)',lambda x: x.sum()),
    col_91= ('TA7_LRNO (number)',lambda x: x.sum()),
    col_92= ('TA8_LRNO (number)',lambda x: x.sum()),
    col_93= ('TA9_LRNO (number)',lambda x: x.sum()),
    col_94= ('TA10_LRNO (number)',lambda x: x.sum()),
    col_95= ('Paging Received(4G) (number)',lambda x: x.sum()),
    col_96= ('Paging Discard(4G) (number)',lambda x: x.sum()),
    col_97= ('LPagingCSFB (number)',lambda x: x.sum()),
    col_98= ('UL Interference_Max_LRNO (number)',lambda x: x.mean()),
    col_99= ('UL Interference_Avg_LRNO (number)',lambda x: x.mean()),
    col_100= ('Mean Number of Users in a Cell_LRNO (number)',lambda x: x.sum()),
    col_101= ('L.E-RAB.AbnormRel.Radio (None)',lambda x: x.sum()),
    col_102= ('L.E-RAB.AbnormRel.Radio.SRBReset (None)',lambda x: x.sum()),
    col_103= ('L.E-RAB.AbnormRel.Radio.DRBReset (None)',lambda x: x.sum()),
    col_104= ('L.E-RAB.AbnormRel.Radio.ULSyncFail (None)',lambda x: x.sum()),
    col_105= ('L.E-RAB.AbnormRel.Radio.UuNoReply (None)',lambda x: x.sum()),
    col_106= ('L.E-RAB.AbnormRel.TNL (None)',lambda x: x.sum()),
    col_107= ('L.E-RAB.AbnormRel.MME (None)',lambda x: x.sum()),
    col_108= ('L.E-RAB.AbnormRel.MME.HOOut (None)',lambda x: x.sum()),
    col_109= ('L.E-RAB.AbnormRel.HOFailure (None)',lambda x: x.sum()),
    col_110= ('L.E-RAB.AbnormRel.MMETot (None)',lambda x: x.sum()),
    col_111= ('L.E-RAB.AbnormRel (None)',lambda x: x.sum()),
    col_112= ('L.E-RAB.AbnormRel.eNBTot (None)',lambda x: x.sum()),
    col_113= ('L.E-RAB.NormRel (None)',lambda x: x.sum()),
    col_114= ('L.E-RAB.AbnormRel.HOOut (None)',lambda x: x.sum()),
    col_115= ('L.UECNTX.NormRel (None)',lambda x: x.sum()),
    col_116= ('L.UECNTX.AbnormRel (None)',lambda x: x.sum()),
    col_117= ('L.UECNTX.AbnormRel.Act (None)',lambda x: x.sum()),
    col_118= ('L.UECNTX.AbnormRel.UlWeak (None)',lambda x: x.sum()),
    col_119= ('L.E-RAB.Release.Unsyn (None)',lambda x: x.sum()),
    col_120= ('L.E-RAB.Num.Syn2Unsyn (None)',lambda x: x.sum()),
    col_121= ('L.RRC.StateTrans.Syn2Unsyn (None)',lambda x: x.sum()),
    col_122= ('L.RRC.StateTrans.Unsyn2Syn (None)',lambda x: x.sum()),
    col_123= ('L.RRC.StateTrans.Unsyn2Syn.Succ (None)',lambda x: x.sum()),
    col_124= ('L.E-RAB.StateTrans.Unsyn2Syn.Att (None)',lambda x: x.sum()),
    col_125= ('L.E-RAB.StateTrans.Unsyn2Syn.Succ (None)',lambda x: x.sum()),
    col_126= ('L.E-RAB.Rel.MME (None)',lambda x: x.sum()),
    col_127= ('L.E-RAB.Left (None)',lambda x: x.sum()),
    col_128= ('L.E-RAB.Rel.eNodeB.Userinact (None)',lambda x: x.sum()),
    col_129= ('L.E-RAB.NormRel.IRatHOOut (None)',lambda x: x.sum()),
    col_130= ('L.RRC.ReEst.Att (None)',lambda x: x.sum()),
    col_131= ('L.RRC.ReEst.Succ (None)',lambda x: x.sum()),
    col_132= ('L.DLPwr.Max (dBm)',lambda x: x.mean()),
    col_133= ('L.DLPwr.Avg (dBm)',lambda x: x.mean()),
    col_134= ('L.Traffic.User.BorderUE.Max (None)',lambda x: x.sum()),
    col_135= ('L.Traffic.User.BorderUE.Avg (None)',lambda x: x.sum()),
    col_136= ('L.Traffic.User.SRS.Avg (None)',lambda x: x.sum()),
    col_137= ('L.Traffic.User.SRS.Max (None)',lambda x: x.sum()),
    col_138= ('L.Traffic.User.Ulsync.Avg (None)',lambda x: x.sum()),
    col_139= ('L.Traffic.User.Ulsync.Max (None)',lambda x: x.sum()),
    col_140= ('L.LC.User.Rel (None)',lambda x: x.sum()),
    col_141= ('L.LC.DLCong.Dur.Cell (s)',lambda x: x.sum()),
    col_142= ('L.LC.ULCong.Dur.Cell (s)',lambda x: x.sum()),
    col_143= ('L.LC.DLCong.Num.Cell (None)',lambda x: x.sum()),
    col_144= ('L.LC.ULCong.Num.Cell (None)',lambda x: x.sum()),
    col_145= ('L_DL_IBLER_N (%)',lambda x: x.sum()),
    col_146= ('L_DL_IBLER_D (%)',lambda x: x.sum()),
    col_147= ('L.Traffic.DL.SCH.QPSK.TB (None)',lambda x: x.sum()),
    col_148= ('L.Traffic.DL.SCH.16QAM.TB (None)',lambda x: x.sum()),
    col_149= ('L.Traffic.DL.SCH.64QAM.TB (None)',lambda x: x.sum()),
    col_150= ('L.Traffic.DL.SCH.64QAM.DRB.TB (None)',lambda x: x.sum()),
    col_151= ('L.Traffic.DL.SCH.16QAM.DRB.TB (None)',lambda x: x.sum()),
    col_152= ('L.Traffic.DL.SCH.QPSK.DRB.TB (None)',lambda x: x.sum()),
    col_153= ('L.Traffic.UL.SCH.QPSK.TB (None)',lambda x: x.sum()),
    col_154= ('L.Traffic.UL.SCH.16QAM.TB (None)',lambda x: x.sum()),
    col_155= ('L.Traffic.UL.SCH.64QAM.TB (None)',lambda x: x.sum()),
    col_156= ('L.Traffic.UL.SCH.QPSK.ErrTB.Ibler (None)',lambda x: x.sum()),
    col_157= ('L.Traffic.UL.SCH.16QAM.ErrTB.Ibler (None)',lambda x: x.sum()),
    col_158= ('L.Traffic.UL.SCH.64QAM.ErrTB.Ibler (None)',lambda x: x.sum()),
    col_159= ('L.DLPwr.Max (dBm)',lambda x: x.max()),
    col_160= ('L.DLPwr.Avg (dBm)',lambda x: x.max()),
    col_161= ('L_PDSCH MCS_N (%)',lambda x: x.sum()),
    col_162= ('L_PDSCH_MCS_D (%)',lambda x: x.sum()),
    col_163= ('L.ChMeas.CQI.DL.0 (None)',lambda x: x.sum()),
    col_164= ('L.ChMeas.CQI.DL.1 (None)',lambda x: x.sum()),
    col_165= ('L.ChMeas.CQI.DL.2 (None)',lambda x: x.sum()),
    col_166= ('L.ChMeas.CQI.DL.3 (None)',lambda x: x.sum()),
    col_167= ('L.ChMeas.CQI.DL.4 (None)',lambda x: x.sum()),
    col_168= ('L.ChMeas.CQI.DL.5 (None)',lambda x: x.sum()),
    col_169= ('L.ChMeas.CQI.DL.6 (None)',lambda x: x.sum()),
    col_170= ('L.ChMeas.CQI.DL.7 (None)',lambda x: x.sum()),
    col_171= ('L.ChMeas.CQI.DL.8 (None)',lambda x: x.sum()),
    col_172= ('L.ChMeas.CQI.DL.9 (None)',lambda x: x.sum()),
    col_173= ('L.ChMeas.CQI.DL.10 (None)',lambda x: x.sum()),
    col_174= ('L.ChMeas.CQI.DL.11 (None)',lambda x: x.sum()),
    col_175= ('L.ChMeas.CQI.DL.12 (None)',lambda x: x.sum()),
    col_176= ('L.ChMeas.CQI.DL.13 (None)',lambda x: x.sum()),
    col_177= ('L.ChMeas.CQI.DL.14 (None)',lambda x: x.sum()),
    col_178= ('L.ChMeas.CQI.DL.15 (None)',lambda x: x.sum()),
    col_179= ('L.CSFB.E2W.Idle (None)',lambda x: x.sum()),
    col_180= ('L.CSFB.PrepSucc.Idle (None)',lambda x: x.sum()),
    col_181= ('L.CSFB.PrepAtt.Idle (None)',lambda x: x.sum()),
    col_182= ('L.RRCRedirection.E2W.CSFB.TimeOut (None)',lambda x: x.sum()),
    col_183= ('L.Thrp.bits.DL.LastTTI (bit)',lambda x: x.sum()),
    col_184= ('L.Thrp.Time.DL.RmvLastTTI (ms)',lambda x: x.sum()),
    col_185= ('L.Thrp.bits.UE.UL.LastTTI (bit)',lambda x: x.sum()),
    col_186= ('L.Thrp.Time.UE.UL.RmvLastTTI (ms)',lambda x: x.sum()),
    col_187= ('L.ChMeas.PDCCH.SymNum.1 (None)',lambda x: x.sum()),
    col_188= ('L.ChMeas.PDCCH.SymNum.2 (None)',lambda x: x.sum()),
    col_189= ('L.ChMeas.PDCCH.SymNum.3 (None)',lambda x: x.sum()),
    col_190= ('L.ChMeas.PDCCH.SymNum.4 (None)',lambda x: x.sum()),
    col_191= ('L.ChMeas.CCE.Avail (None)',lambda x: x.sum()),
    col_192= ('L.ChMeas.CCE.Avail.Max (None)',lambda x: x.sum()),
    col_193= ('L.ChMeas.CCE.DL.AllocFail (None)',lambda x: x.sum()),
    col_194= ('L.ChMeas.CCE.CommUsed (None)',lambda x: x.sum()),
    col_195= ('L.ChMeas.CCE.DLUsed (None)',lambda x: x.sum()),
    col_196= ('L.ChMeas.CCE.ULUsed (None)',lambda x: x.sum()),
    col_197= ('L.ChMeas.CCE.UL.Avail.Max (None)',lambda x: x.sum()),
    col_198= ('L.ChMeas.CCE.UL.AllocFail (None)',lambda x: x.sum()),
    col_199= ('L.ChMeas.PRB.PUSCH.Avg (None)',lambda x: x.sum()),
    col_200= ('L.ChMeas.PRB.PUSCH.Avg.BorderUE (None)',lambda x: x.sum()),
    col_201= ('L.UL.RSRP.PUSCH.Index0 (None)',lambda x: x.sum()),
    col_202= ('L.UL.RSRP.PUSCH.Index1 (None)',lambda x: x.sum()),
    col_203= ('L.UL.RSRP.PUSCH.Index2 (None)',lambda x: x.sum()),
    col_204= ('L.UL.RSRP.PUSCH.Index3 (None)',lambda x: x.sum()),
    col_205= ('L.UL.RSRP.PUSCH.Index4 (None)',lambda x: x.sum()),
    col_206= ('L.UL.RSRP.PUSCH.Index5 (None)',lambda x: x.sum()),
    col_207= ('L.UL.RSRP.PUSCH.Index6 (None)',lambda x: x.sum()),
    col_208= ('L.UL.RSRP.PUSCH.Index7 (None)',lambda x: x.sum()),
    col_209= ('L.UL.RSRP.PUSCH.Index8 (None)',lambda x: x.sum()),
    col_210= ('L.UL.RSRP.PUSCH.Index9 (None)',lambda x: x.sum()),
    col_211= ('L.UL.RSRP.PUSCH.Index10 (None)',lambda x: x.sum()),
    col_212= ('L.UL.RSRP.PUSCH.Index11 (None)',lambda x: x.sum()),
    col_213= ('L.UL.RSRP.PUSCH.Index12 (None)',lambda x: x.sum()),
    col_214= ('L.UL.RSRP.PUSCH.Index13 (None)',lambda x: x.sum()),
    col_215= ('L.UL.RSRP.PUSCH.Index14 (None)',lambda x: x.sum()),
    col_216= ('L.UL.RSRP.PUSCH.Index15 (None)',lambda x: x.sum()),
    col_217= ('L.UL.RSRP.PUSCH.Index16 (None)',lambda x: x.sum()),
    col_218= ('L.UL.RSRP.PUSCH.Index17 (None)',lambda x: x.sum()),
    col_219= ('L.UL.RSRP.PUSCH.Index18 (None)',lambda x: x.sum()),
    col_220= ('L.UL.RSRP.PUSCH.Index19 (None)',lambda x: x.sum()),
    col_221= ('L.UL.RSRP.PUSCH.Index20 (None)',lambda x: x.sum()),
    col_222= ('L.UL.RSRP.PUSCH.Index21 (None)',lambda x: x.sum()),
    col_223= ('L.UL.RSRP.PUSCH.Index22 (None)',lambda x: x.sum()),
    col_224= ('L.UL.RSRP.PUSCH.Index23 (None)',lambda x: x.sum()),
    col_225= ('L_DL_ReTrans_N (%)',lambda x: x.sum()),
    col_226= ('L_DL_ReTrans_D (%)',lambda x: x.sum()),
    col_227= ('L_UL_ReTrans_N (%)',lambda x: x.sum()),
    col_228= ('L_UL_ReTrans_D (%)',lambda x: x.sum()),
    col_229= ('Mean Number of Users in a Cell_LRNO (number)',lambda x: x.max()),
    col_230= ('Average PRB Utilization (DL)_LRNO (%)',lambda x: x.max()),
    col_231= ('Average PRB Utilization (UL)_LRNO (%)',lambda x: x.max()),
    col_232= ('Total Traffic (Mbit)_LRNO (MB)',lambda x: x.max()),
    col_233= ('UL Interference_Avg_LRNO (number)',lambda x: x.max()),
    col_234= ('L.Traffic.DL.PktDelay.Time (ms)',lambda x: x.sum()),
    col_235= ('L.Traffic.DL.PktDelay.Num (packet)',lambda x: x.sum()),
    col_236= ('L.ChMeas.RI.Rank1 (None)',lambda x: x.sum()),
    col_237= ('L.ChMeas.RI.Rank2 (None)',lambda x: x.sum()),
    col_238= ('Average PRB Utilization (DL)_N_LRNO (number)',lambda x: x.max()),
    col_239= ('Average PRB Utilization (DL)_D_LRNO (number)',lambda x: x.max()),
    col_240= ('DL Traffic (Mbit)_LRNO (MB)',lambda x: x.max()),
    col_241= ('L.E-RAB.FailEst.MME (None)',lambda x: x.sum()),
    col_242= ('L.E-RAB.FailEst.NoRadioRes (None)',lambda x: x.sum()),
    col_243= ('L.E-RAB.FailEst.RNL (None)',lambda x: x.sum()),
    col_244= ('L.E-RAB.FailEst.TNL (None)',lambda x: x.sum()),
    col_245= ('L.Paging.Dis.Num (None)',lambda x: x.sum()),
    col_246= ('L.RRC.ConnSetup.TimeAvg (ms)',lambda x: x.mean()),
    col_247= ('L.E-RAB.Est.TimeAvg (ms)',lambda x: x.mean()),
    col_248= ('L.E-RAB.FailEst.NoReply (None)',lambda x: x.sum()),
    col_249= ('L.Paging.UU.Succ (None)',lambda x: x.sum()),
    col_250= ('L.Paging.UU.Succ.CSFB (None)',lambda x: x.sum()),

).reset_index().rename(columns={'col_4': 'Total Data Volume(4G)', 
                                'col_5': 'DL Data Vol_MB(4G)',
                                'col_6': 'UL Data Vol_MB(4G)',
                                'col_11': 'Mean Number Of User(4G)',
                                'col_12': 'Max Number of User(4G)',
                                'col_19': 'CSFB(E2W)', 
                                'col_20': 'CSFB(E2G)',
                                'col_27': 'Cell Unavailble Time(s)',
                                'col_32': 'SumOfTotal Traffic (Mbit)_LRNO (MB)',
                                'col_33': 'SumOfUL Traffic (Mbit)_LRNO (MB)',
                                'col_34': 'SumOfDL Traffic (Mbit)_LRNO (MB)',
                                'col_35': 'SumOfCall  Setup Success Rate_N_LRNO (number)', 
                                'col_36': 'SumOfCall  Setup Success Rate_D_LRNO (number)',
                                'col_37': 'SumOfUser Throughput in DownLink (Mbps)_N_LRNO (number)',
                                'col_38': 'SumOfUser Throughput in DownLink (Mbps)_D_LRNO (number)',
                                'col_39': 'SumOfUser Throughput in UpLink (Mbps)_N_LRNO (number)',
                                'col_40': 'SumOfUser Throughput in UpLink (Mbps)_D_LRNO (number)',
                                'col_41': 'AvgOfMean Number of Users in a Cell_LRNO (number)',
                                'col_42': 'MaxOfMaximum Number of Users(Cell)_LRNO (number)',
                                'col_43': 'SumOfPS Drop Call Rate_N_LRNO (number)',
                                'col_44': 'SumOfPS Drop Call Rate_D_LRNO (number)',
                                'col_45': 'SumOfPS E UTRAN RAB Setup Success Rate_N_LRNO (number)',
                                'col_46': 'SumOfPS E UTRAN RAB Setup Success Rate_D_LRNO (number)',
                                'col_47': 'SumOfPS E UTRAN RRC Setup successful Ratio_N_LRNO (number)',
                                'col_48': 'SumOfPS E UTRAN RRC Setup successful Ratio_D_LRNO (number)',
                                'col_49': 'SumOfDL Packet Loss Rate_N_LRNO (number)',
                                'col_50': 'SumOfDL Packet Loss Rate_D_LRNO (number)',
                                'col_51': 'SumOfUL Packet Loss Rate_N_LRNO (number)',
                                'col_52': 'SumOfUL Packet Loss Rate_D_LRNO (number)',
                                'col_53': 'SumOfAverage PRB Utilization (DL)_N_LRNO (number)',
                                'col_54': 'SumOfAverage PRB Utilization (DL)_D_LRNO (number)',
                                'col_55': 'SumOfAverage PRB Utilization (UL)_N_LRNO (number)',
                                'col_56': 'SumOfAverage PRB Utilization (UL)_D_LRNO (number)',
                                'col_57': 'SumOfHandover over X2 success Ratio_N_LRNO (number)',
                                'col_58': 'SumOfHandover over X2 success Ratio_D_LRNO (number)',
                                'col_59': 'SumOfIntra Frequency Handover Ratio_N_LRNO (number)',
                                'col_60': 'SumOfIntra Frequency Handover Ratio_D_LRNO (number)',
                                'col_61': 'SumOfInter RAT Handover out Success Rate (L2W)_N_LRNO (number)',
                                'col_62': 'SumOfInter RAT Handover out Success Rate (L2W)_D_LRNO (number)',
                                'col_63': 'SumOfInter RAT Redirection In Success Rate (L2G)_N_LRNO (number)',
                                'col_64': 'SumOfInter RAT Redirection In Success Rate (L2W)_N_LRNO (number)',
                                'col_65': 'SumOfS1 Sig Setup Success Rate_N_LRNO (number)',
                                'col_66': 'SumOfS1 Sig Setup Success Rate_D_LRNO (number)',
                                'col_67': 'SumOfDL PDCP Throughput (Mbit/s)_N_LRNO (Mbit/s)',
                                'col_68': 'SumOfDL PDCP Throughput (Mbit/s)_D_LRNO (Mbit/s)',
                                'col_69': 'SumOfUL PDCP Throughput (Mbit/s)_N_LRNO (Mbit/s)',
                                'col_70': 'SumOfUL PDCP Throughput (Mbit/s)_D_LRNO (number)',
                                'col_71': 'SumOfL.CSFB.E2W (None)',
                                'col_72': 'SumOfL.CSFB.E2G (None)',
                                'col_73': 'SumOfL.RRCRedirection.E2W.CSFB (None)',
                                'col_74': 'SumOfL.RRCRedirection.E2G.CSFB (None)',
                                'col_75': 'SumOfL.CSFB.PrepSucc (None)',
                                'col_76': 'SumOfL.CSFB.PrepAtt (None)',
                                'col_77': 'SumOfSecurityMod_N_LRNO (number)',
                                'col_78': 'SumOfSecurityMod_D_LRNO (number)',
                                'col_79': 'SumOfHandover In Success Rate_N_LRNO (number)',
                                'col_80': 'SumOfHandover In Success Rate_D_LRNO (number)',
                                'col_81': 'SumOfCellUnavailTime_LRNO (s)',
                                'col_82': 'SumOfERAB_Cong_N_LRNO (number)',
                                'col_83': 'SumOfERAB_CONG_D_LRNO (number)',
                                'col_84': 'SumOfTA0_LRNO (number)',
                                'col_85': 'SumOfTA1_LRNO (number)',
                                'col_86': 'SumOfTA2_LRNO (number)',
                                'col_87': 'SumOfTA3_LRNO (number)',
                                'col_88': 'SumOfTA4_LRNO (number)',
                                'col_89': 'SumOfTA5_LRNO (number)',
                                'col_90': 'SumOfTA6_LRNO (number)',
                                'col_91': 'SumOfTA7_LRNO (number)',
                                'col_92': 'SumOfTA8_LRNO (number)',
                                'col_93': 'SumOfTA9_LRNO (number)',
                                'col_94': 'SumOfTA10_LRNO (number)',
                                'col_95': 'SumOfPaging Received(4G) (number)',
                                'col_96': 'SumOfPaging Discard(4G) (number)',
                                'col_97': 'SumOfLPagingCSFB (number)',
                                'col_98': 'AvgOfUL Interference_Max_LRNO',

                                'col_99': 'AvgOfUL Interference_Avg_LRNO',
                                'col_100': 'SumOfMean Number of Users in a Cell_LRNO (number)',
                                'col_101': 'SumOfL.E-RAB.AbnormRel.Radio (None)',
                                'col_102': 'SumOfL.E-RAB.AbnormRel.Radio.SRBReset (None)',
                                'col_103': 'SumOfL.E-RAB.AbnormRel.Radio.DRBReset (None)',
                                'col_104': 'SumOfL.E-RAB.AbnormRel.Radio.ULSyncFail (None)',
                                'col_105': 'SumOfL.E-RAB.AbnormRel.Radio.UuNoReply (None)',
                                'col_106': 'SumOfL.E-RAB.AbnormRel.TNL (None)',
                                'col_107': 'SumOfL.E-RAB.AbnormRel.MME (None)',
                                'col_108': 'SumOfL.E-RAB.AbnormRel.MME.HOOut (None)',
                                'col_109': 'SumOfL.E-RAB.AbnormRel.HOFailure (None)',
                                'col_110': 'SumOfL.E-RAB.AbnormRel.MMETot (None)',
                                'col_111': 'SumOfL.E-RAB.AbnormRel (None)',
                                'col_112': 'SumOfL.E-RAB.AbnormRel.eNBTot (None)',
                                'col_113': 'SumOfL.E-RAB.NormRel (None)',
                                'col_114': 'SumOfL.E-RAB.AbnormRel.HOOut (None)',
                                'col_115': 'SumOfL.UECNTX.NormRel (None)',
                                'col_116': 'SumOfL.UECNTX.AbnormRel (None)',
                                'col_117': 'SumOfL.UECNTX.AbnormRel.Act (None)',
                                'col_118': 'SumOfL.UECNTX.AbnormRel.UlWeak (None)',
                                'col_119': 'SumOfL.E-RAB.Release.Unsyn (None)',
                                'col_120': 'SumOfL.E-RAB.Num.Syn2Unsyn (None)',
                                'col_121': 'SumOfL.RRC.StateTrans.Syn2Unsyn (None)',
                                'col_122': 'SumOfL.RRC.StateTrans.Unsyn2Syn (None)',
                                'col_123': 'SumOfL.RRC.StateTrans.Unsyn2Syn.Succ (None)',
                                'col_124': 'SumOfL.E-RAB.StateTrans.Unsyn2Syn.Att (None)',
                                'col_125': 'SumOfL.E-RAB.StateTrans.Unsyn2Syn.Succ (None)',
                                'col_126': 'SumOfL.E-RAB.Rel.MME (None)',
                                'col_127': 'SumOfL.E-RAB.Left (None)',
                                'col_128': 'SumOfL.E-RAB.Rel.eNodeB.Userinact (None)',
                                'col_129': 'SumOfL.E-RAB.NormRel.IRatHOOut (None)',
                                'col_130': 'SumOfL.RRC.ReEst.Att (None)',
                                'col_131': 'AvgOfL.RRC.ReEst.Succ (None)',
                                'col_132': 'AvgOfL.DLPwr.Max (dBm)',
                                'col_133': 'AvgOfL.DLPwr.Avg (dBm)',
                                'col_134': 'SumOfL.Traffic.User.BorderUE.Max (None)',
                                'col_135': 'SumOfL.Traffic.User.BorderUE.Avg (None)',
                                'col_136': 'SumOfL.Traffic.User.SRS.Avg (None)',
                                'col_137': 'SumOfL.Traffic.User.SRS.Max (None)',
                                'col_138': 'SumOfL.Traffic.User.Ulsync.Avg (None)',
                                'col_139': 'SumOfL.Traffic.User.Ulsync.Max (None)',
                                'col_140': 'SumOfL.LC.User.Rel (None)',
                                'col_141': 'SumOfL.LC.DLCong.Dur.Cell (s)',
                                'col_142': 'SumOfL.LC.ULCong.Dur.Cell (s)',
                                'col_143': 'SumOfL.LC.DLCong.Num.Cell (None)',
                                'col_144': 'SumOfL.LC.ULCong.Num.Cell (None)',
                                'col_145': 'SumOfL_DL_IBLER_N (%)',
                                'col_146': 'SumOfL_DL_IBLER_D (%)',
                                'col_147': 'SumOfL.Traffic.DL.SCH.QPSK.TB (None)',
                                'col_148': 'SumOfL.Traffic.DL.SCH.16QAM.TB (None)',
                                'col_149': 'SumOfL.Traffic.DL.SCH.64QAM.TB (None)',
                                'col_150': 'SumOfL.Traffic.DL.SCH.64QAM.DRB.TB (None)',
                                'col_151': 'SumOfL.Traffic.DL.SCH.16QAM.DRB.TB (None)',
                                'col_152': 'SumOfL.Traffic.DL.SCH.QPSK.DRB.TB (None)',
                                'col_153': 'SumOfL.Traffic.UL.SCH.QPSK.TB (None)',
                                'col_154': 'SumOfL.Traffic.UL.SCH.16QAM.TB (None)',
                                'col_155': 'SumOfL.Traffic.UL.SCH.64QAM.TB (None)',
                                'col_156': 'SumOfL.Traffic.UL.SCH.QPSK.ErrTB.Ibler (None)',
                                'col_157': 'SumOfL.Traffic.UL.SCH.16QAM.ErrTB.Ibler (None)',
                                'col_158': 'SumOfL.Traffic.UL.SCH.64QAM.ErrTB.Ibler (None)',
                                'col_159': 'MaxOfL.DLPwr.Max (dBm)',
                                'col_160': 'MaxOfL.DLPwr.Avg (dBm)',
                                'col_161': 'SumOfL_PDSCH MCS_N (%)',
                                'col_162': 'SumOfL_PDSCH_MCS_D (%)',
                                'col_163': 'SumOfL.ChMeas.CQI.DL.0 (None)',
                                'col_164': 'SumOfL.ChMeas.CQI.DL.1 (None)',
                                'col_165': 'SumOfL.ChMeas.CQI.DL.2 (None)',
                                'col_166': 'SumOfL.ChMeas.CQI.DL.3 (None)',
                                'col_167': 'SumOfL.ChMeas.CQI.DL.4 (None)',
                                'col_168': 'SumOfL.ChMeas.CQI.DL.5 (None)',
                                'col_169': 'SumOfL.ChMeas.CQI.DL.6 (None)',
                                'col_170': 'SumOfL.ChMeas.CQI.DL.7 (None)',
                                'col_171': 'SumOfL.ChMeas.CQI.DL.8 (None)',
                                'col_172': 'SumOfL.ChMeas.CQI.DL.9 (None)',
                                'col_173': 'SumOfL.ChMeas.CQI.DL.10 (None)',
                                'col_174': 'SumOfL.ChMeas.CQI.DL.11 (None)',
                                'col_175': 'SumOfL.ChMeas.CQI.DL.12 (None)',
                                'col_176': 'SumOfL.ChMeas.CQI.DL.13 (None)',
                                'col_177': 'SumOfL.ChMeas.CQI.DL.14 (None)',
                                'col_178': 'SumOfL.ChMeas.CQI.DL.15 (None)',
                                'col_179': 'SumOfL.CSFB.E2W.Idle (None)',
                                'col_180': 'SumOfL.CSFB.PrepSucc.Idle (None)',
                                'col_181': 'SumOfL.CSFB.PrepAtt.Idle (None)',
                                'col_182': 'SumOfL.RRCRedirection.E2W.CSFB.TimeOut (None)',
                                'col_183': 'SumOfL.Thrp.bits.DL.LastTTI (bit)',
                                'col_184': 'SumOfL.Thrp.Time.DL.RmvLastTTI (ms)',
                                'col_185': 'SumOfL.Thrp.bits.UE.UL.LastTTI (bit)',
                                'col_186': 'SumOfL.Thrp.Time.UE.UL.RmvLastTTI (ms)',
                                'col_187': 'SumOfL.ChMeas.PDCCH.SymNum.1 (None)',
                                'col_188': 'SumOfL.ChMeas.PDCCH.SymNum.2 (None)',
                                'col_189': 'SumOfL.ChMeas.PDCCH.SymNum.3 (None)',
                                'col_190': 'SumOfL.ChMeas.PDCCH.SymNum.4 (None)',
                                'col_191': 'SumOfL.ChMeas.CCE.Avail (None)',
                                'col_192': 'SumOfL.ChMeas.CCE.Avail.Max (None)',
                                'col_193': 'SumOfL.ChMeas.CCE.DL.AllocFail (None)',
                                'col_194': 'SumOfL.ChMeas.CCE.CommUsed (None)',
                                'col_195': 'SumOfL.ChMeas.CCE.DLUsed (None)',
                                'col_196': 'SumOfL.ChMeas.CCE.ULUsed (None)',
                                'col_197': 'SumOfL.ChMeas.CCE.UL.Avail.Max (None)',
                                'col_198': 'SumOfL.ChMeas.CCE.UL.AllocFail (None)',
                                'col_199': 'SumOfL.ChMeas.PRB.PUSCH.Avg (None)',
                                'col_200': 'SumOfL.ChMeas.PRB.PUSCH.Avg.BorderUE (None)',
                                'col_201': 'SumOfL.UL.RSRP.PUSCH.Index0 (None)',
                                'col_202': 'SumOfL.UL.RSRP.PUSCH.Index1 (None)',
                                'col_203': 'SumOfL.UL.RSRP.PUSCH.Index2 (None)',
                                'col_204': 'SumOfL.UL.RSRP.PUSCH.Index3 (None)',
                                'col_205': 'SumOfL.UL.RSRP.PUSCH.Index4 (None)',
                                'col_206': 'SumOfL.UL.RSRP.PUSCH.Index5 (None)',
                                'col_207': 'SumOfL.UL.RSRP.PUSCH.Index6 (None)',
                                'col_208': 'SumOfL.UL.RSRP.PUSCH.Index7 (None)',
                                'col_209': 'SumOfL.UL.RSRP.PUSCH.Index8 (None)',
                                'col_210': 'SumOfL.UL.RSRP.PUSCH.Index9 (None)',
                                'col_211': 'SumOfL.UL.RSRP.PUSCH.Index10 (None)',
                                'col_212': 'SumOfL.UL.RSRP.PUSCH.Index11 (None)',
                                'col_213': 'SumOfL.UL.RSRP.PUSCH.Index12 (None)',
                                'col_214': 'SumOfL.UL.RSRP.PUSCH.Index13 (None)',
                                'col_215': 'SumOfL.UL.RSRP.PUSCH.Index14 (None)',
                                'col_216': 'SumOfL.UL.RSRP.PUSCH.Index15 (None)',
                                'col_217': 'SumOfL.UL.RSRP.PUSCH.Index16 (None)',
                                'col_218': 'SumOfL.UL.RSRP.PUSCH.Index17 (None)',
                                'col_219': 'SumOfL.UL.RSRP.PUSCH.Index18 (None)',
                                'col_220': 'SumOfL.UL.RSRP.PUSCH.Index19 (None)',
                                'col_221': 'SumOfL.UL.RSRP.PUSCH.Index20 (None)',
                                'col_222': 'SumOfL.UL.RSRP.PUSCH.Index21 (None)',
                                'col_223': 'SumOfL.UL.RSRP.PUSCH.Index22 (None)',
                                'col_224': 'SumOfL.UL.RSRP.PUSCH.Index23 (None)',
                                'col_225': 'SumOfL_DL_ReTrans_N (%)',
                                'col_226': 'SumOfL_DL_ReTrans_D (%)',
                                'col_227': 'SumOfL_UL_ReTrans_N (%)',
                                'col_228': 'SumOfL_UL_ReTrans_D (%)',
                                'col_229': 'MaxOfMean Number of Users in a Cell_LRNO (number)',
                                'col_230': 'MaxOfAverage PRB Utilization (DL)_LRNO (%)',
                                'col_231': 'MaxOfAverage PRB Utilization (UL)_LRNO (%)',
                                'col_232': 'MaxOfTotal Traffic (Mbit)_LRNO (MB)',
                                'col_233': 'MaxOfUL Interference_Avg_LRNO (number)',
                                'col_234': 'L.Traffic.DL.PktDelay.Time (ms)',
                                'col_235': 'L.Traffic.DL.PktDelay.Num (packet)',
                                'col_236': 'L.ChMeas.RI.Rank1 (None)',
                                'col_237': 'L.ChMeas.RI.Rank2 (None)',
                                'col_238': 'MaxOfAverage PRB Utilization (DL)_N_LRNO (number)',
                                'col_239': 'MaxOfAverage PRB Utilization (DL)_D_LRNO (number)',
                                'col_240': 'MaxofDL Traffic(Mbit)',
                                'col_241': 'SumOfL.E-RAB.FailEst.MME (None)',
                                'col_242': 'SumOfL.E-RAB.FailEst.NoRadioRes (None)',
                                'col_243': 'SumOfL.E-RAB.FailEst.RNL (None)',
                                'col_244': 'SumOfL.E-RAB.FailEst.TNL (None)',
                                'col_245': 'SumOfL.Paging.Dis.Num (None)',
                                'col_246': 'AvgOfL.RRC.ConnSetup.TimeAvg (ms)',
                                'col_247': 'AvgOfL.E-RAB.Est.TimeAvg (ms)',
                                'col_248': 'SumOfL.E-RAB.FailEst.NoReply (None)',
                                'col_249': 'SumOfL.Paging.UU.Succ (None)',
                                'col_250': 'SumOfL.Paging.UU.Succ.CSFB (None)',
                                })





df['DL User ThrpT_Mbps(4G)']=df['col_7a']/df['col_7b']
df['col_7a']= df['DL User ThrpT_Mbps(4G)']
df.rename(columns={'col_7a': 'DL User ThrpT_Mbps(4G)'}, inplace=True)
# df = df.iloc[:, :-1]



df['UL User ThrpT_Mbps(4G)']=df['col_8a']/df['col_8b']
df['col_8a']= df['UL User ThrpT_Mbps(4G)']
df.rename(columns={'col_8a': 'UL User ThrpT_Mbps(4G)'}, inplace=True)



df['DL PDCP ThrpT_Mbps']=df['col_9a']/df['col_9b']
df['col_9a']= df['DL PDCP ThrpT_Mbps']
df.rename(columns={'col_9a': 'DL PDCP ThrpT_Mbps'}, inplace=True)


df['UL PDCP Throughput_Mbps']=df['col_10a']/df['col_10b']
df['col_10a']= df['UL PDCP Throughput_Mbps']
df.rename(columns={'col_10a': 'UL PDCP Throughput_Mbps'}, inplace=True)

df['Call  Setup SR(4G)']=(df['col_13a']/df['col_13b'])*100
df['col_13a']= df['Call  Setup SR(4G)']
df.rename(columns={'col_13a': 'Call  Setup SR(4G)'}, inplace=True)

df['PS EUTRAN RRC SR']=(df['col_14a']/df['col_14b'])*100
df['col_14a']= df['PS EUTRAN RRC SR']
df.rename(columns={'col_14a': 'PS EUTRAN RRC SR'}, inplace=True)

df['PS E UTRAN RAB SR']=(df['col_15a']/df['col_15b'])*100
df['col_15a']= df['PS E UTRAN RAB SR']
df.rename(columns={'col_15a': 'PS E UTRAN RAB SR'}, inplace=True)

df['S1 Signaling SR']=(df['col_16a']/df['col_16b'])*100
df['col_16a']= df['S1 Signaling SR']
df.rename(columns={'col_16a': 'S1 Signaling SR'}, inplace=True)


df['SecurityMod SR']=(df['col_17a']/df['col_17b'])*100
df['col_17a']= df['SecurityMod SR']
df.rename(columns={'col_17a': 'SecurityMod SR'}, inplace=True)

df['PS Drop Rate(4G)']=(df['col_18a']/df['col_18b'])*100
df['col_18a']= df['PS Drop Rate(4G)']
df.rename(columns={'col_18a': 'PS Drop Rate(4G)'}, inplace=True)

df['CSFBPrepSR']=(df['col_21a']/df['col_21b'])*100
df['col_21a']= df['CSFBPrepSR']
df.rename(columns={'col_21a': 'CSFBPrepSR'}, inplace=True)

df['PRB Utilization_DL']=(df['col_22a']/df['col_22b'])*100
df['col_22a']= df['PRB Utilization_DL']
df.rename(columns={'col_22a': 'PRB Utilization_DL'}, inplace=True)

df['PRB Utilization_UL']=(df['col_23a']/df['col_23b'])*100
df['col_23a']= df['PRB Utilization_UL']
df.rename(columns={'col_23a': 'PRB Utilization_UL'}, inplace=True)

df['ERAB_CONG RATE']=(df['col_24a']/df['col_24b'])*100
df['col_24a']= df['ERAB_CONG RATE']
df.rename(columns={'col_24a': 'ERAB_CONG RATE'}, inplace=True)

df['DL Packet Loss Rate']=(df['col_25a']/df['col_25b'])*100
df['col_25a']= df['DL Packet Loss Rate']
df.rename(columns={'col_25a': 'DL Packet Loss Rate'}, inplace=True)

df['UL Packet Loss Rate']=(df['col_26a']/df['col_26b'])*100
df['col_26a']= df['UL Packet Loss Rate']
df.rename(columns={'col_26a': 'UL Packet Loss Rate'}, inplace=True)

df['Handover over X2 SR']=(df['col_28a']/df['col_28b'])*100
df['col_28a']= df['Handover over X2 SR']
df.rename(columns={'col_28a': 'Handover over X2 SR'}, inplace=True)

df['Intra Frequency Handover SR']=(df['col_29a']/df['col_29b'])*100
df['col_29a']= df['Intra Frequency Handover SR']
df.rename(columns={'col_29a': 'Intra Frequency Handover SR'}, inplace=True)

df['Inter RAT Handover out SR (L2W)']=(df['col_30a']/df['col_30b'])*100
df['col_30a']= df['Inter RAT Handover out SR (L2W)']
df.rename(columns={'col_30a': 'Inter RAT Handover out SR (L2W)'}, inplace=True)

df['Handover In SR']=(df['col_31a']/df['col_31b'])*100
df['col_31a']= df['Handover In SR']
df.rename(columns={'col_31a': 'Handover In SR'}, inplace=True)

df.drop(['col_7b', 'col_8b', 'col_9b', 'col_10b', 'col_13b', 'col_14b', 'col_15b', 
         'col_16b', 'col_17b', 'col_18b', 'col_21b', 'col_22b', 'col_23b', 
         'col_24b', 'col_25b', 'col_26b', 'col_28b', 'col_28b', 'col_29b', 
         'col_30b', 'col_31b'], axis=1, inplace=True)

df = df.iloc[:, :-20]


df.to_csv(os.path.join(path, 'Daily.csv'), index=False)

endtime = time.time()

print((endtime-starttime)/60)
