import argparse
import logging
import math
import os

import numpy as np
import pandas as pd
import pyodbc as sql
from matplotlib import pyplot as plt

import Queries as q


def aggregate_evals(src, typ, tctype, rating, eval_group, color):
	if typ == 'ACPL':
		qry_text = q.eval_acpl(src, tctype, rating, eval_group, color)
	else:
		N = typ[1:2]
		qry_text = q.eval_tx(src, tctype, rating, eval_group, color, N)
	conn_str = q.get_conf('SqlServerConnectionStringTrusted')
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
		# plt_name = f'{typ}_{rating}_{tctype}_{plt_type}.png'
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
		# 	plt.title(f'Type: {typ}, Rating: {rating}, TimeControlType = {tctype}, {ci_max}% confidence interval')
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
		# 	plt.title(f'Type: {typ}, Rating: {rating}, TimeControlType = {tctype}, Whiskers = {whis}')
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

def aggregate_event(typ, tctype, rating):
	if typ == 'ACPL':
		qry_text = q.event_acpl(tctype, rating)
	elif typ == 'SDCPL':
		qry_text = q.event_sdcpl(tctype, rating)
	elif typ == 'Score':
		qry_text = q.event_score(tctype, rating)
	else:
		N = typ[1:2]
		qry_text = q.event_tx(tctype, rating, N)
	conn_str = q.get_conf('SqlServerConnectionStringTrusted')
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
		# plt_name = f'{typ}_{rating}_{tctype}_{plt_type}.png'
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
		# 	plt.title(f'Type: {typ}, Rating: {rating}, TimeControlType = {tctype}, {ci_max}% confidence interval')
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
		# 	plt.title(f'Type: {typ}, Rating: {rating}, TimeControlType = {tctype}, Whiskers = {whis}')
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

def aggregate_game(src, typ, tctype, rating, color):
	if typ == 'ACPL':
		qry_text = q.game_acpl(src, tctype, rating, color)
	elif typ == 'SDCPL':
		qry_text = q.game_sdcpl(src, tctype, rating, color)
	elif typ == 'Score':
		qry_text = q.game_score(src, tctype, rating, color)
	else:
		N = typ[1:2]
		qry_text = q.game_tx(tctype, rating, color, N)
	conn_str = q.get_conf('SqlServerConnectionStringTrusted')
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

		if typ == 'Score' and qtr3 > 100:
			qtr3 = 100
		if typ == 'Score' and upper_pcnt > 100:
			upper_pcnt = 100


		# plt_type = 'box' # [hist, box]
		# plt_path = f'C:\\Users\\eehunt\\Repository\\EngineDetection\\plots\\{plt_type}'
		# plt_name = f'{typ}_{rating}_{tctype}_{plt_type}.png'
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
		# 	plt.title(f'Type: {typ}, Rating: {rating}, TimeControlType = {tctype}, {ci_max}% confidence interval')
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
		# 	plt.title(f'Type: {typ}, Rating: {rating}, TimeControlType = {tctype}, Whiskers = {whis}')
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

def evaluation(src, agg):
	conn = sql.connect(q.get_conf('SqlServerConnectionStringTrusted'))
	csr = conn.cursor()

	sql_cmd = f"DELETE FROM StatisticsSummary WHERE Source = '{src}' AND Aggregation = '{agg}'"
	csr.execute(sql_cmd)
	conn.commit()

	if src == 'Control':
		tc_list = ['Classical', 'Correspondence']
		rating_dict = {'min': 1200, 'max': 2900}
	elif src == 'Lichess':
		tc_list = ['Rapid', 'Classical']
		rating_dict = {'min': 2200, 'max': 3400}

	# this takes a long time to run FYI
	for typ in ['ACPL', 'T1', 'T2', 'T3', 'T4', 'T5']:
		rating = rating_dict['min']
		while rating < rating_dict['max']:
			for tc_type in tc_list:
				for color in ['White', 'Black']:
					for i in range(9):
						eval_group = i + 1
						ct, av, sd, lower, qt1, qt2, qt3, upper = aggregate_evals(src, typ, tc_type, rating, eval_group, color)
						sql_cmd = 'INSERT INTO StatisticsSummary (Source, Aggregation, Field, Rating, TimeControlType, Color, EvalGroup, Count, Average, StandardDeviation, LowerPcnt, LowerQuartile, Median, UpperQuartile, UpperPcnt) '
						sql_cmd = sql_cmd + f"VALUES ('{src}', '{agg}', '{typ}', {rating}, '{tc_type}', '{color}', {eval_group}, {ct}, {av}, {sd}, {lower}, {qt1}, {qt2}, {qt3}, {upper})"
						csr.execute(sql_cmd)
						conn.commit()
						logging.info(f'Done with type = {typ}, min_rating = {rating}, eval_group = {eval_group}, color = {color}, timecontroltype = {tc_type}')
			rating = rating + 100
	conn.close()

def event(src, agg):
	conn = sql.connect(q.get_conf('SqlServerConnectionStringTrusted'))
	csr = conn.cursor()

	sql_cmd = f"DELETE FROM StatisticsSummary WHERE Source = '{src}' AND Aggregation = '{agg}'"
	csr.execute(sql_cmd)
	conn.commit()

	if src == 'Control':
		tc_list = ['Classical', 'Correspondence']
		rating_dict = {'min': 1200, 'max': 2900}
	elif src == 'Lichess':
		tc_list = ['Rapid', 'Classical']
		rating_dict = {'min': 2200, 'max': 3400}

	for typ in ['ACPL', 'SDCPL', 'T1', 'T2', 'T3', 'T4', 'T5', 'Score']:
		rating = rating_dict['min']
		while rating < rating_dict['max']:
			for tc_type in tc_list:
				ct, av, sd, lower, qt1, qt2, qt3, upper = aggregate_event(typ, tc_type, rating)
				sql_cmd = 'INSERT INTO StatisticsSummary (Source, Aggregation, Field, Rating, TimeControlType, Color, EvalGroup, Count, Average, StandardDeviation, LowerPcnt, LowerQuartile, Median, UpperQuartile, UpperPcnt) '
				sql_cmd = sql_cmd + f"VALUES ('{src}', '{agg}', '{typ}', {rating}, '{tc_type}', 'N/A', 0, {ct}, {av}, {sd}, {lower}, {qt1}, {qt2}, {qt3}, {upper})"
				csr.execute(sql_cmd)
				conn.commit()
				logging.info(f'Done with type = {typ}, rating = {rating}, timecontroltype = {tc_type}')
			rating = rating + 100
	conn.close()

def game(src, agg):
	conn = sql.connect(q.get_conf('SqlServerConnectionStringTrusted'))
	csr = conn.cursor()

	sql_cmd = f"DELETE FROM StatisticsSummary WHERE Source = '{src}' AND Aggregation = '{agg}'"
	csr.execute(sql_cmd)
	conn.commit()

	if src == 'Control':
		tc_list = ['Classical', 'Correspondence']
		rating_dict = {'min': 1200, 'max': 2900}
	elif src == 'Lichess':
		tc_list = ['Rapid', 'Classical']
		rating_dict = {'min': 2200, 'max': 3400}

	for typ in ['ACPL', 'SDCPL', 'T1', 'T2', 'T3', 'T4', 'T5', 'Score']:
		rating = rating_dict['min']
		while rating < rating_dict['max']:
			for tc_type in tc_list:
				for color in ['White', 'Black']:
					ct, av, sd, lower, qt1, qt2, qt3, upper = aggregate_game(src, typ, tc_type, rating, color)
					sql_cmd = 'INSERT INTO StatisticsSummary (Source, Aggregation, Field, Rating, TimeControlType, Color, EvalGroup, Count, Average, StandardDeviation, LowerPcnt, LowerQuartile, Median, UpperQuartile, UpperPcnt) '
					sql_cmd = sql_cmd + f"VALUES ('{src}', '{agg}', '{typ}', {rating}, '{tc_type}', '{color}', 0, {ct}, {av}, {sd}, {lower}, {qt1}, {qt2}, {qt3}, {upper})"
					csr.execute(sql_cmd)
					conn.commit()
					logging.info(f'Done with type = {typ}, rating = {rating}, corr_flag = {tc_type}, color = {color}')
			rating = rating + 100

	conn.close()

def main():
	logging.basicConfig(format='%(funcName)s\t%(levelname)s\t%(message)s', level=logging.INFO)

	vrs_num = '1.0'
	parser = argparse.ArgumentParser(
		description = 'Control Statistic Aggregator',
		formatter_class = argparse.ArgumentDefaultsHelpFormatter,
		usage = argparse.SUPPRESS
	)
	parser.add_argument(
		'-v', '--version',
		action = 'version',
		version = '%(prog)s ' + vrs_num
	)
	parser.add_argument(
		'-t', '--typ',
		default = 'Event',
		choices = ['Evaluation', 'Event', 'Game'],
		help = 'Aggregation level'
	)
	parser.add_argument(
		'-s', '--src',
		default = 'Control',
		choices = ['Control', 'Lichess'],
		help = 'Data source'
	)

	args = parser.parse_args()
	config = vars(args)
	typ = config['typ']
	src = config['src']
	
	if typ == 'Evaluation':
		evaluation(src, typ)
	elif typ == 'Event':
		if src == 'Lichess':
			logging.critical('Lichess not developed for event analysis!')
			raise SystemExit
		else:
			event(src, typ)
	elif typ == 'Game':
		game(src, typ)


if __name__ == '__main__':
    main()
