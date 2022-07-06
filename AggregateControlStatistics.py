import math
import os

import numpy as np
import pandas as pd
import pyodbc as sql
from matplotlib import pyplot as plt

import Queries as q


def aggregate_evals(typ, corr_flag, rating, eval_group, color):
	if typ == 'ACPL':
		qry_text = q.eval_acpl(corr_flag, rating, eval_group, color)
	else:
		N = typ[1:2]
		qry_text = q.eval_tx(corr_flag, rating, eval_group, color, N)
	conn_str = q.get_connstr()
	conn = sql.connect(conn_str)
	data_np = pd.read_sql(qry_text, conn).to_numpy()
	conn.close()
	if len(data_np) > 0:
		data_arr = np.concatenate(data_np)

		ct = len(data_arr)
		av = np.mean(data_arr)
		sd = np.std(data_arr)

		ci_min = 1
		ci_max = 100 - ci_min
		lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.percentile(data_arr, [ci_min, 25, 50, 75, ci_max])

		# plt_type = 'box' # [hist, box]
		# plt_path = f'C:\\Users\\eehunt\\Repository\\EngineDetection\\plots\\{plt_type}'
		# plt_name = f'{typ}_{rating}_{corr_flag}_{plt_type}.png'
		# plt_full = os.path.join(plt_path, plt_name)
		# if typ in ['ACPL', 'SDCPL']:
		# 	mn = 0
		# 	mx = math.ceil(np.max(data_arr)*100)/100
		# 	b = np.arange(mn, mx, 0.01)
		# elif typ == 'Score':
		# 	mn = math.floor(np.min(data_arr))
		# 	mx = math.ceil(np.max(data_arr))
		# 	b = np.arange(mn, mx, 0.25)
		# else:
		# 	mn = math.floor(np.min(data_arr)*100)/100
		# 	mx = math.ceil(np.max(data_arr)*100)/100
		# 	b = np.arange(mn, mx, 0.005)
		
		# if plt_type == 'hist':
		# 	plt.figure('Histogram')
		# 	plt.hist(data_arr, bins=b)
		# 	plt.xlim(mn, mx*1.1)
		# 	plt.title(f'Type: {typ}, Rating: {rating}, Corr_Flag = {corr_flag}, {ci_max}% confidence interval')
		# 	plt.axvline(lower_pcnt, color='k', linestyle='dashed', linewidth=1)
		# 	plt.axvline(av, color='red', linestyle='dashed', linewidth=1)
		# 	plt.axvline(upper_pcnt, color='k', linestyle='dashed', linewidth=1)
		# 	plt.text(mn + 0.01, plt.ylim()[1]*0.95, 'Lower CI: {:.2f}'.format(lower_pcnt))
		# 	plt.text(mn + 0.01, plt.ylim()[1]*0.91, 'Mean:      {:.2f}'.format(av))
		# 	plt.text(mn + 0.01, plt.ylim()[1]*0.87, 'Upper CI: {:.2f}'.format(upper_pcnt))
		# else:
		# 	plt.figure('Boxplot')
		# 	whis = [ci_min, ci_max]
		# 	bp = plt.boxplot(data_arr, vert=0, whis=whis, sym='+')
		# 	plt.title(f'Type: {typ}, Rating: {rating}, Corr_Flag = {corr_flag}, Whiskers = {whis}')
		# 	plt.xlim(mn, mx*1.1)

		# 	lwhis = bp['whiskers'][0].get_xdata()[1]
		# 	q1 = bp['boxes'][0].get_xdata()[1]
		# 	q2 = bp['medians'][0].get_xdata()[1]
		# 	q3 = bp['boxes'][0].get_xdata()[2]
		# 	uwhis = bp['whiskers'][1].get_xdata()[1]
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.97, 'Lower Whisker: {:.2f}'.format(lwhis))
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.94, 'Lower Quartile: {:.2f}'.format(q1))
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.91, 'Median:             {:.2f}'.format(q2))
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.88, 'Upper Quartile: {:.2f}'.format(q3))
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.85, 'Upper Whisker: {:.2f}'.format(uwhis))
		# plt.savefig(plt_full)
		# # plt.show()
		# plt.clf()
	else:
		ct = 0
		av = 'NULL'
		sd = 'NULL'
		lower_pcnt = 'NULL'
		qtr1 = 'NULL'
		qtr2 = 'NULL'
		qtr3 = 'NULL'
		upper_pcnt = 'NULL'

	return [ct, av, sd, lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt]

def aggregate_event(typ, corrflag, rating):
	if typ == 'ACPL':
		qry_text = q.event_acpl(corrflag, rating)
	elif typ == 'SDCPL':
		qry_text = q.event_sdcpl(corrflag, rating)
	elif typ == 'Score':
		qry_text = q.event_score(corrflag, rating)
	else:
		N = typ[1:2]
		qry_text = q.event_tx(corrflag, rating, N)
	conn_str = q.get_connstr()
	conn = sql.connect(conn_str)
	data_np = pd.read_sql(qry_text, conn).to_numpy()
	conn.close()
	if len(data_np) > 0:
		data_arr = np.concatenate(data_np)

		ct = len(data_arr)
		av = np.mean(data_arr)
		sd = np.std(data_arr)

		ci_min = 1
		ci_max = 100 - ci_min
		lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.percentile(data_arr, [ci_min, 25, 50, 75, ci_max])

		# plt_type = 'box' # [hist, box]
		# plt_path = f'C:\\Users\\eehunt\\Repository\\EngineDetection\\plots\\{plt_type}'
		# plt_name = f'{typ}_{rating}_{corrflag}_{plt_type}.png'
		# plt_full = os.path.join(plt_path, plt_name)
		# if typ in ['ACPL', 'SDCPL']:
		# 	mn = 0
		# 	mx = math.ceil(np.max(data_arr)*100)/100
		# 	b = np.arange(mn, mx, 0.01)
		# elif typ == 'Score':
		# 	mn = math.floor(np.min(data_arr))
		# 	mx = math.ceil(np.max(data_arr))
		# 	b = np.arange(mn, mx, 0.25)
		# else:
		# 	mn = math.floor(np.min(data_arr)*100)/100
		# 	mx = math.ceil(np.max(data_arr)*100)/100
		# 	b = np.arange(mn, mx, 0.005)
		
		# if plt_type == 'hist':
		# 	plt.figure('Histogram')
		# 	plt.hist(data_arr, bins=b)
		# 	plt.xlim(mn, mx*1.1)
		# 	plt.title(f'Type: {typ}, Rating: {rating}, Corr_Flag = {corrflag}, {ci_max}% confidence interval')
		# 	plt.axvline(lower_pcnt, color='k', linestyle='dashed', linewidth=1)
		# 	plt.axvline(av, color='red', linestyle='dashed', linewidth=1)
		# 	plt.axvline(upper_pcnt, color='k', linestyle='dashed', linewidth=1)
		# 	plt.text(mn + 0.01, plt.ylim()[1]*0.95, 'Lower CI: {:.2f}'.format(lower_pcnt))
		# 	plt.text(mn + 0.01, plt.ylim()[1]*0.91, 'Mean:      {:.2f}'.format(av))
		# 	plt.text(mn + 0.01, plt.ylim()[1]*0.87, 'Upper CI: {:.2f}'.format(upper_pcnt))
		# else:
		# 	plt.figure('Boxplot')
		# 	whis = [ci_min, ci_max]
		# 	bp = plt.boxplot(data_arr, vert=0, whis=whis, sym='+')
		# 	plt.title(f'Type: {typ}, Rating: {rating}, Corr_Flag = {corrflag}, Whiskers = {whis}')
		# 	plt.xlim(mn, mx*1.1)

		# 	lwhis = bp['whiskers'][0].get_xdata()[1]
		# 	q1 = bp['boxes'][0].get_xdata()[1]
		# 	q2 = bp['medians'][0].get_xdata()[1]
		# 	q3 = bp['boxes'][0].get_xdata()[2]
		# 	uwhis = bp['whiskers'][1].get_xdata()[1]
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.97, 'Lower Whisker: {:.2f}'.format(lwhis))
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.94, 'Lower Quartile: {:.2f}'.format(q1))
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.91, 'Median:             {:.2f}'.format(q2))
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.88, 'Upper Quartile: {:.2f}'.format(q3))
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.85, 'Upper Whisker: {:.2f}'.format(uwhis))
		# plt.savefig(plt_full)
		# # plt.show()
		# plt.clf()
	else:
		ct = 0
		av = 'NULL'
		sd = 'NULL'
		lower_pcnt = 'NULL'
		qtr1 = 'NULL'
		qtr2 = 'NULL'
		qtr3 = 'NULL'
		upper_pcnt = 'NULL'

	return [ct, av, sd, lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt]

def aggregate_game(typ, corrflag, rating, color):
	if typ == 'ACPL':
		qry_text = q.game_acpl(corrflag, rating, color)
	elif typ == 'SDCPL':
		qry_text = q.game_sdcpl(corrflag, rating, color)
	elif typ == 'Score':
		qry_text = q.game_score(corrflag, rating, color)
	else:
		N = typ[1:2]
		qry_text = q.game_tx(corrflag, rating, color, N)
	conn_str = q.get_connstr()
	conn = sql.connect(conn_str)
	data_np = pd.read_sql(qry_text, conn).to_numpy()
	conn.close()
	if len(data_np) > 0:
		data_arr = np.concatenate(data_np)

		ct = len(data_arr)
		av = np.mean(data_arr)
		sd = np.std(data_arr)

		ci_min = 1
		ci_max = 100 - ci_min
		lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.percentile(data_arr, [ci_min, 25, 50, 75, ci_max])

		# plt_type = 'box' # [hist, box]
		# plt_path = f'C:\\Users\\eehunt\\Repository\\EngineDetection\\plots\\{plt_type}'
		# plt_name = f'{typ}_{rating}_{corrflag}_{plt_type}.png'
		# plt_full = os.path.join(plt_path, plt_name)
		# if typ in ['ACPL', 'SDCPL']:
		# 	mn = 0
		# 	mx = math.ceil(np.max(data_arr)*100)/100
		# 	b = np.arange(mn, mx, 0.01)
		# elif typ == 'Score':
		# 	mn = math.floor(np.min(data_arr))
		# 	mx = math.ceil(np.max(data_arr))
		# 	b = np.arange(mn, mx, 0.25)
		# else:
		# 	mn = math.floor(np.min(data_arr)*100)/100
		# 	mx = math.ceil(np.max(data_arr)*100)/100
		# 	b = np.arange(mn, mx, 0.005)
		
		# if plt_type == 'hist':
		# 	plt.figure('Histogram')
		# 	plt.hist(data_arr, bins=b)
		# 	plt.xlim(mn, mx*1.1)
		# 	plt.title(f'Type: {typ}, Rating: {rating}, Corr_Flag = {corrflag}, {ci_max}% confidence interval')
		# 	plt.axvline(lower_pcnt, color='k', linestyle='dashed', linewidth=1)
		# 	plt.axvline(av, color='red', linestyle='dashed', linewidth=1)
		# 	plt.axvline(upper_pcnt, color='k', linestyle='dashed', linewidth=1)
		# 	plt.text(mn + 0.01, plt.ylim()[1]*0.95, 'Lower CI: {:.2f}'.format(lower_pcnt))
		# 	plt.text(mn + 0.01, plt.ylim()[1]*0.91, 'Mean:      {:.2f}'.format(av))
		# 	plt.text(mn + 0.01, plt.ylim()[1]*0.87, 'Upper CI: {:.2f}'.format(upper_pcnt))
		# else:
		# 	plt.figure('Boxplot')
		# 	whis = [ci_min, ci_max]
		# 	bp = plt.boxplot(data_arr, vert=0, whis=whis, sym='+')
		# 	plt.title(f'Type: {typ}, Rating: {rating}, Corr_Flag = {corrflag}, Whiskers = {whis}')
		# 	plt.xlim(mn, mx*1.1)

		# 	lwhis = bp['whiskers'][0].get_xdata()[1]
		# 	q1 = bp['boxes'][0].get_xdata()[1]
		# 	q2 = bp['medians'][0].get_xdata()[1]
		# 	q3 = bp['boxes'][0].get_xdata()[2]
		# 	uwhis = bp['whiskers'][1].get_xdata()[1]
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.97, 'Lower Whisker: {:.2f}'.format(lwhis))
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.94, 'Lower Quartile: {:.2f}'.format(q1))
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.91, 'Median:             {:.2f}'.format(q2))
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.88, 'Upper Quartile: {:.2f}'.format(q3))
		# 	plt.text(mx*1.1*0.68, plt.ylim()[1]*0.85, 'Upper Whisker: {:.2f}'.format(uwhis))
		# plt.savefig(plt_full)
		# # plt.show()
		# plt.clf()
	else:
		ct = 0
		av = 'NULL'
		sd = 'NULL'
		lower_pcnt = 'NULL'
		qtr1 = 'NULL'
		qtr2 = 'NULL'
		qtr3 = 'NULL'
		upper_pcnt = 'NULL'

	return [ct, av, sd, lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt]

def game(agg):
	conn = sql.connect(q.get_connstr())
	csr = conn.cursor()

	sql_cmd = f"DELETE FROM StatisticsSummary WHERE Aggregation = '{agg}'"
	csr.execute(sql_cmd)
	conn.commit()

	for typ in ['ACPL', 'SDCPL', 'T1', 'T2', 'T3', 'T4', 'T5', 'Score']:
		rating = 1200
		while rating < 2900:
			for corr_flag in ['0', '1']:
				for color in ['White', 'Black']:
					ct, av, sd, lower, qt1, qt2, qt3, upper = aggregate_game(typ, corr_flag, rating, color)
					sql_cmd = 'INSERT INTO StatisticsSummary (Aggregation, Field, Rating, CorrFlag, Color, EvalGroup, Count, Average, StandardDeviation, LowerPcnt, LowerQuartile, Median, UpperQuartile, UpperPcnt) '
					sql_cmd = sql_cmd + f"VALUES ('{agg}', '{typ}', {rating}, {corr_flag}, '{color}', 0, {ct}, {av}, {sd}, {lower}, {qt1}, {qt2}, {qt3}, {upper})"
					csr.execute(sql_cmd)
					conn.commit()
					print(f'{agg}: Done with type = {typ}, rating = {rating}, corr_flag = {corr_flag}, color = {color}')
			rating = rating + 100
	conn.close()

def event(agg):
	conn = sql.connect(q.get_connstr())
	csr = conn.cursor()

	sql_cmd = f"DELETE FROM StatisticsSummary WHERE Aggregation = '{agg}'"
	csr.execute(sql_cmd)
	conn.commit()
	for typ in ['ACPL', 'SDCPL', 'T1', 'T2', 'T3', 'T4', 'T5', 'Score']:
		rating = 1200
		while rating < 2900:
			for corr_flag in ['0', '1']:
				ct, av, sd, lower, qt1, qt2, qt3, upper = aggregate_event(typ, corr_flag, rating)
				sql_cmd = 'INSERT INTO StatisticsSummary (Aggregation, Field, Rating, CorrFlag, Color, EvalGroup, Count, Average, StandardDeviation, LowerPcnt, LowerQuartile, Median, UpperQuartile, UpperPcnt) '
				sql_cmd = sql_cmd + f"VALUES ('{agg}', '{typ}', {rating}, {corr_flag}, 'N/A', 0, {ct}, {av}, {sd}, {lower}, {qt1}, {qt2}, {qt3}, {upper})"
				csr.execute(sql_cmd)
				conn.commit()
				print(f'{agg}: Done with type = {typ}, rating = {rating}, corr_flag = {corr_flag}')
			rating = rating + 100
	conn.close()

def evaluation(agg):
	conn = sql.connect(q.get_connstr())
	csr = conn.cursor()

	sql_cmd = f"DELETE FROM StatisticsSummary WHERE Aggregation = '{agg}'"
	csr.execute(sql_cmd)
	conn.commit()

	# this takes a long time to run FYI
	for typ in ['ACPL', 'T1', 'T2', 'T3', 'T4', 'T5']:
		rating = 1200
		while rating < 2900:
			for corr_flag in ['0', '1']:
				for color in ['White', 'Black']:
					for i in range(9):
						eval_group = i + 1
						ct, av, sd, lower, qt1, qt2, qt3, upper = aggregate_event(typ, corr_flag, rating, eval_group, color)
						sql_cmd = 'INSERT INTO StatisticsSummary (Aggregation, Field, Rating, CorrFlag, Color, EvalGroup, Count, Average, StandardDeviation, LowerPcnt, LowerQuartile, Median, UpperQuartile, UpperPcnt) '
						sql_cmd = sql_cmd + f"VALUES ('{agg}', '{typ}', {rating}, {corr_flag}, '{color}', {eval_group}, {ct}, {av}, {sd}, {lower}, {qt1}, {qt2}, {qt3}, {upper})"
						csr.execute(sql_cmd)
						conn.commit()
						print(f'{agg}: Done with type = {typ}, min_rating = {rating}, eval_group = {eval_group}, color = {color}, corr_flag = {corr_flag}')
			rating = rating + 100
	conn.close()

def main():
	typ = 'Game' # [Game, Event, Evaluation]
	if typ == 'Game':
		game(typ)
	elif typ == 'Event':
		event(typ)
	elif typ == 'Evaluation':
		evaluation(typ)


if __name__ == '__main__':
    main()
