import pandas as pd
import pyodbc as sql
from matplotlib import pyplot as plt

import Queries as q


def main():
    typ = 'Control'  # EEH/Control/Online/CheatTest
    lastname = 'Carlsen'
    firstname = 'Magnus'
    tmnt = None
    roundnum = None
    color = None  # White/Black
    startdate = None  # will need to validate date format
    enddate = None  # will need to validate date format
    result = None  # 1/0/0.5
    qry = q.construct_test(typ, lastname, firstname, tmnt, roundnum, color, startdate, enddate, result)

    conn_str = q.get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)
    test_df = pd.read_sql(qry, conn)
    test_dict = test_df.to_dict('records')[0]  # returning a list of dicts, one for each record returned in query

    data_qry = 'SELECT * FROM StatisticsSummary'
    data_df = pd.read_sql(data_qry, conn)

    dfsrc = 'Control'
    dfagg = 'Event'
    dffld = 'T1'
    dfrating = 2700
    dftctype = 'Classical'
    dfcolor = 'N/A'
    dfeval = 0
    cond1 = data_df['Source'] == dfsrc
    cond2 = data_df['Aggregation'] == dfagg
    cond3 = data_df['Field'] == dffld
    cond4 = data_df['Rating'] == dfrating
    cond5 = data_df['TimeControlType'] == dftctype
    cond6 = data_df['Color'] == dfcolor
    cond7 = data_df['EvalGroup'] == dfeval
    comp_df = data_df[(cond1) & (cond2) & (cond3) & (cond4) & (cond5) & (cond6) & (cond7)]
    comp_dict = comp_df.to_dict('records')[0]

    plot = False
    if plot:
        # boxplot for comparison
        stats = [{
            # "label": 'Engine Detection Boxplot',
            "mean": comp_dict['Average'],
            "med": comp_dict['Median'],
            "q1": comp_dict['LowerQuartile'],
            "q3": comp_dict['UpperQuartile'],
            "whislo": comp_dict['LowerPcnt'],
            "whishi": comp_dict['UpperPcnt'],
            "fliers": []
            }
        ]

        axes = plt.subplots(nrows=1, ncols=1, figsize=(6, 6), sharey=True)[1]
        axes.bxp(stats)
        plt.axhline(y=test_dict[dffld], color='b', linestyle='-')
        plt.text(plt.xlim()[1]*0.77, plt.ylim()[1]*0.97, 'Lower Whisker: {:.2f}'.format(comp_dict['LowerPcnt']))
        plt.text(plt.xlim()[1]*0.77, plt.ylim()[1]*0.94, 'Lower Quartile: {:.2f}'.format(comp_dict['LowerQuartile']))
        plt.text(plt.xlim()[1]*0.77, plt.ylim()[1]*0.91, 'Median:             {:.2f}'.format(comp_dict['Median']))
        plt.text(plt.xlim()[1]*0.77, plt.ylim()[1]*0.88, 'Upper Quartile: {:.2f}'.format(comp_dict['UpperQuartile']))
        plt.text(plt.xlim()[1]*0.77, plt.ylim()[1]*0.85, 'Upper Whisker: {:.2f}'.format(comp_dict['UpperPcnt']))
        plt.text(plt.xlim()[1]*0.77, plt.ylim()[1]*0.79, 'Test Statistic: {:.2f}'.format(test_dict[dffld]))
        axes.set_title(f'Engine Detection Boxplot, testing {dffld}')
        plt.show()
    else:
        # compare test statistic with appropriate whisker for engine comparison decision
        decision_val = 0
        test_val = test_dict[dffld]
        if dffld in ['ACPL', 'SDCPL']:
            comp_val = comp_dict['LowerPcnt']
            if test_val < comp_val:
                decision_val = 1
        else:
            comp_val = comp_dict['UpperPcnt']
            if test_val > comp_val:
                decision_val = 1

        if decision_val:
            print(f'Outperformed {dffld}! Test statistic: {test_val}, Compare statistic: {comp_val}, Rating Level: {dfrating}, Time Control: {dftctype}, Aggregation: {dfagg}')
        else:
            print(f'Standard {dffld} performance. Test statistic: {test_val}, Compare statistic: {comp_val}, Rating Level: {dfrating}, Time Control: {dftctype}, Aggregation: {dfagg}')


if __name__ == '__main__':
    main()
