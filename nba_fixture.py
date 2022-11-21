import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime
import itertools as it


# _today = pd.Timestamp.now().floor("D") - pd.Timedelta(days=7)
_today = pd.Timestamp.now().floor("D")
_from = _today + pd.Timedelta(days=0)
_to = _from + pd.Timedelta(days=1 * 7)
_to_p = _to - pd.Timedelta(days=_to.weekday())
_date_range = pd.date_range(_from, _to, inclusive="left").tolist()

FLAG = True
top_store = list()
while FLAG:
    dt1 = _date_range.pop(0)
    dt1_s = dt1.strftime("%Y%m%d")
    target_url = f"https://www.espn.com/nba/schedule/_/date/{dt1_s}"
    response = requests.get(target_url)
    response.raise_for_status()
    html_content = response.content
    # with open(f"nba_fixture/html_store/{dt1_s}.html", "wb") as file:
    #     file.write(html_content)
    soup = BeautifulSoup(html_content, "lxml")
    l1_li = soup.find_all(
        "div",
        {"class": "ScheduleTables mb5 ScheduleTables--nba ScheduleTables--basketball"},
    )
    store = list()
    for l1 in l1_li:
        # head = l1.find("thead", {"class": "Table__THEAD"})
        _date = l1.find("div", class_="Table__Title").text.strip()
        _date_p = datetime.datetime.strptime(_date, "%A, %B %d, %Y")

        body = l1.find("tbody", {"class": "Table__TBODY"})
        away = body.find_all("span", class_="Table__Team away")
        home = body.find_all("span", class_="Table__Team")
        _time = body.find_all("td", class_="date__col Table__TD")

        away_p = list(map(lambda x: x.text.strip(), away))
        home_p = list(map(lambda x: x.text.strip(), home))
        home_p2 = list(filter(lambda x: x not in away_p, home_p))
        _time_p = list(map(lambda x: x.text.strip(), _time))

        _datetime_p = list(it.product([_date], _time_p))
        _datetime_p2 = list(
            map(
                lambda x: datetime.datetime.strptime(
                    f"{x[0]} {x[1]}", "%A, %B %d, %Y %I:%M %p"
                ),
                _datetime_p,
            )
        )

        away_home_zip = list(zip(away_p, home_p2))
        pld1 = pd.DataFrame(away_home_zip, columns=["away", "home"])
        pld1.insert(loc=0, column="dt", value=_datetime_p2)
        pld1 = pld1[["dt", "away", "home"]]
        store.append(pld1)

    pld2 = pd.concat(store)
    top_store.append(pld2)
    # min_date = pld2["dt"].min().floor("D")
    max_date = pld2["dt"].max().floor("D")
    _date_range = list(filter(lambda x: x > max_date, _date_range))
    if len(_date_range) < 1:
        FLAG = False
df = pd.concat(top_store)
min_date = df["dt"].min().floor("D").strftime("%m%d%y")
max_date = df["dt"].max().floor("D").strftime("%m%d%y")

ranking = pd.read_csv("/Users/nikhilsoni/Documents/projects/sandbox/nba_fixture/ranking_2022_11_21.csv")
ranking.loc[:, "dt"] = pd.to_datetime(ranking["dt"])
ranking = ranking.rename(columns={"dt": "rdt"})

df1 = df.merge(ranking, how="left", left_on="away", right_on="sched_code")
df1 = df1.drop(columns=["raw", "team", "code", "sched_code", "rdt"])
df1 = df1.rename(columns={"pos": "away_pos", "conf": "away_conf"})
df1 = df1[["dt", "away_conf", "away", "away_pos", "home"]]
df2 = df1.merge(ranking, how="left", left_on="home", right_on="sched_code")
df2 = df2.drop(columns=["raw", "team", "code", "sched_code"])
df2 = df2.rename(columns={"pos": "home_pos", "conf": "home_conf"})
df2 = df2[["rdt", "dt", "away_conf", "away", "away_pos", "home_conf", "home", "home_pos"]]
df2.loc[:, "rdt"] = df2["rdt"].apply(lambda x: x.strftime("%m/%d"))
dt = df2["dt"].copy()
df2.loc[:, "dt"] = dt.apply(lambda x: x.strftime("%m/%d"))
df2.insert(loc=0, column="day", value=dt.apply(lambda x: x.strftime("%a")))
df2.insert(loc=0, column="time", value=dt.apply(lambda x: (x-pd.Timedelta(hours=3)).strftime("%H:%M")))
df2 = df2[["rdt", "dt", "day", "time", "away_conf", "away", "away_pos", "home_conf", "home", "home_pos"]]

le5 = df2["away_pos"].le(5) & df2["home_pos"].le(5)
gsw = (df2["away"] == "Golden State") | (df2["home"] == "Golden State")
fltr = le5 | gsw
df3 = df2[fltr].copy()

fpath = f"nba_fixture/SCHED-{min_date}-{max_date}.csv"
df3.to_csv(fpath, index=False) # Final result

debug = True
