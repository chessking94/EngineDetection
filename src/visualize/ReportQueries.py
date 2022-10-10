import json
import matplotlib.pyplot as plt

import pandas as pd
import pyodbc as sql


def get_conf(key):
    fname = r'C:\Users\eehunt\Repository\confidential.json'
    with open(fname, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


class lichess:
    def __init__(self):
        pass

    def complete(self):
        return 'SELECT * FROM vwLichessData'

    def game(self, gameid):
        return f"SELECT * FROM vwLichessData WHERE GameID = '{gameid}'"


def main():
    conn_str = get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)
    lc = lichess()
    gameid = 'mndd4DfW'
    qry = lc.game(gameid)
    df = pd.read_sql(qry, conn)
    conn.close()

    df_white = df[df['Color'] == 'White']
    df_black = df[df['Color'] == 'Black']

    fig, ax = plt.subplots()
    plt.plot(df_white['MoveNumber'], df_white['Clock']/5)
    plt.plot(df_black['MoveNumber'], df_black['Clock']*-1/5)
    plt.bar(x=df_white['MoveNumber'], height=df_white['TimeSpent'], width=1)
    plt.bar(x=df_black['MoveNumber'], height=df_black['TimeSpent']*-1, width=1)

    # look into a way to normalize the time remaining graph? i.e. take logs

    plt.show()


if __name__ == '__main__':
    main()
