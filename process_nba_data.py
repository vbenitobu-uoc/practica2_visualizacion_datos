import pandas as pd
import numpy as np
from io import StringIO   # ← Esta es la corrección clave

# ------------------- 1. CARGAR Y FILTRAR TEMPORADAS -------------------
files = {
    'player_season': 'Player Season Info.csv',
    'player_career': 'Player Career Info.csv',
    'player_totals': 'Player Totals.csv',
    'player_per100': 'Per 100 Poss.csv',
    'all_star': 'All-Star Selections.csv',
    'end_teams': 'End of Season Teams.csv',
    'team_summaries': 'Team Summaries.csv',
    'team_abbrev': 'Team Abbrev.csv',
}

data = {}
for key, file in files.items():
    data[key] = pd.read_csv(file, low_memory=False)
    print(f"{key}: {data[key].shape[0]} filas cargadas")

# temporadas post 2000
season_files = ['player_season', 'player_totals', 'player_per100', 'all_star', 'end_teams', 'team_summaries', 'team_abbrev']
for key in season_files:
    if 'season' in data[key].columns:
        data[key] = data[key][data[key]['season'].between(2001, 2026)]
        print(f"Filtrado {key}: {data[key].shape[0]} filas")

# datos genericos
df = data['player_season'][['season', 'player_id', 'player', 'team', 'pos', 'age', 'experience']].copy()

# Universidad y nacimiento
df = df.merge(
    data['player_career'][['player_id', 'colleges', 'birth_date']],
    on='player_id',
    how='left'
)
# Lista Jugadores
intl_players_csv = """
player_name,birth_country,debut_year,nationality,notes
Álex Abrines,Spain,2016,Spain,
Al Horford,Dominican Republic,2007,Dominican Republic,
Anderson Varejão,Brazil,2004,Brazil,
Andrés Nocioni,Argentina,2004,Argentina,
Andrew Bogut,Australia,2005,Australia,
Andrew Wiggins,Canada,2014,Canada,
Ángel Delgado,Dominican Republic,2018,Dominican Republic,
Anžejs Pasečņiks,Latvia,2019,Latvia,
Anthony Bennett,Canada,2013,Canada,
Aron Baynes,Australia,2013,Australia,
Arvydas Macijauskas,Lithuania,2005,Lithuania,
Ben Simmons,Australia,2017,Australia,
Bennedict Mathurin,Canada,2022,Canada,
Bismack Biyombo,Democratic Republic of the Congo,2011,DRC,
Boban Marjanović,Serbia,2015,Serbia,
Bogdan Bogdanović,Serbia,2017,Serbia,
Bojan Bogdanović,Croatia,2014,Croatia,
Brandon Clarke,Canada,2019,Canada,
Bruno Fernando,Angola,2019,Angola,
Bruno Šundov,Croatia,1998,Croatia,
Buddy Hield,Bahamas,2016,Bahamas,
Cameron Bairstow,Australia,2014,Australia,
Carlos Arroyo,Puerto Rico,2001,Puerto Rico,
Carlos Delfino,Argentina,2004,Argentina,
Chima Moneke,Nigeria,2022,Nigeria,
Chris Boucher,Canada,2017,Canada,
Chris Duarte,Dominican Republic,2021,Dominican Republic,
Cui Yongxi,China,2024,China,
Dalano Banton,Canada,2021,Canada,
Damir Markota,Croatia,2006,Croatia,
Damjan Rudež,Croatia,2014,Croatia,
Dan Gadzuric,Netherlands,2002,Netherlands,
Danilo Gallinari,Italy,2008,Italy,
Dante Exum,Australia,2014,Australia,
Dāvis Bertāns,Latvia,2016,Latvia,
David Andersen,Australia,2009,Australia,
DeAndre Ayton,Bahamas,2018,Bahamas,
Deng Adel,South Sudan,2019,Australia,
Didi Louzada,Brazil,2021,Brazil,
Dillon Brooks,Canada,2017,Canada,
Duje Dukan,Croatia,2016,Croatia,
Edy Tavares,Cape Verde,2015,Cape Verde,
Emanuel Miller,Canada,2025,Canada,
Emmanuel Mudiay,Democratic Republic of the Congo,2015,DRC,
Fabricio Oberto,Argentina,2005,Argentina,
Facundo Campazzo,Argentina,2020,Argentina,
Felipe López,Dominican Republic,1998,Dominican Republic,
Gabriel Deck,Argentina,2021,Argentina,
Gabriel Lundberg,Denmark,2022,Denmark,
Garth Joseph,Dominica,2000,Dominica,
Gigi Datome,Italy,2013,Italy,
Gordan Giriček,Croatia,2002,Croatia,
Gorgui Dieng,Senegal,2013,Senegal,
Goran Dragić,Slovenia,2008,Slovenia,
Gustavo Ayón,Mexico,2011,Mexico,
Ha Seung-Jin,South Korea,2004,South Korea,
Hamady N'Diaye,Senegal,2010,Senegal,
Hugo González,Spain,2025,Spain,
Ibou Badji,Senegal,2023,Senegal,
Ignas Brazdeikis,Lithuania,2019,Lithuania,
Ivica Zubac,Croatia,2016,Croatia,
Jack White,Australia,2022,Australia,
Jackson Rowe,Canada,2024,Canada,
Jaime Echenique,Colombia,2021,Colombia,
Jamal Murray,Canada,2016,Canada,
Jahmyl Telfort,Canada,2025,Canada,
J. J. Barea,Puerto Rico,2006,Puerto Rico,
Jakob Poeltl,Austria,2016,Austria,
Jonas Valančiūnas,Lithuania,2012,Lithuania,
Jonathan Kuminga,Democratic Republic of the Congo,2021,DRC,
Jordan Nwora,Nigeria,2020,Nigeria,
Josh Giddey,Australia,2021,Australia,
Josh Green,Australia,2020,Australia,
Josh Okogie,Nigeria,2018,Nigeria,
Jusuf Nurkić,Bosnia and Herzegovina,2014,Bosnia and Herzegovina,
Kai Jones,Bahamas,2021,Bahamas,
Karim Mané,Canada,2020,Canada,
Karlo Matković,Croatia,2024,Croatia,
Kasparas Jakučionis,Lithuania,2025,Lithuania,
Khem Birch,Canada,2017,Canada,
Kristaps Porziņģis,Latvia,2015,Latvia,
Kyle Anderson,China,2014,China,
Leandro Barbosa,Brazil,2003,Brazil,
Leonard Miller,Canada,2023,Canada,
Lindell Wigginton,Canada,2022,Canada,
Linas Kleiza,Lithuania,2005,Lithuania,
Luka Dončić,Slovenia,2018,Slovenia,
Luka Šamanić,Croatia,2019,Croatia,
Luis Montero,Dominican Republic,2015,Dominican Republic,
Luis Scola,Argentina,2007,Argentina,
Marc Gasol,Spain,2008,Spain,
Marcin Gortat,Poland,2007,Poland,
Marial Shayok,South Sudan,2019,South Sudan,
Mario Hezonja,Croatia,2015,Croatia,
Marko Gudurić,Serbia,2019,Serbia,
Marquinhos Vieira,Brazil,2006,Brazil,
Mfiondu Kabengele,Canada,2019,Canada,
Mindaugas Kuzminskas,Lithuania,2016,Lithuania,
Mitch Creek,Australia,2019,Australia,
Mouhamed Gueye,Senegal,2023,Senegal,
Mãozinha Pereira,Brazil,2024,Brazil,
Nenê,Brazil,2002,Brazil,
Nenad Krstić,Serbia,2004,Serbia,
Neemias Queta,Portugal,2021,Portugal,
Nemanja Bjelica,Serbia,2015,Serbia,
Nikola Jokić,Serbia,2015,Serbia,
Nikola Vučević,Montenegro,2011,Montenegro,
OG Anunoby,United Kingdom,2017,United Kingdom,
Olivier Sarr,France,2021,France,
Olumide Oyedeji,Nigeria,2000,Nigeria,
Omer Yurtseven,Turkey,2021,Turkey,
Ondřej Balvín,Czech Republic,2016,Czech Republic,
Oscar da Silva,Brazil,2021,Brazil,
Oshae Brissett,Canada,2019,Canada,
Pascal Siakam,Cameroon,2016,Cameroon,
Patty Mills,Australia,2009,Australia,
Paul Zipser,Germany,2016,Germany,
Pau Gasol,Spain,2001,Spain,
Peja Stojaković,Serbia,1998,Serbia,
Peter Jok,South Sudan,2017,South Sudan,
Predrag Savović,Serbia,2002,Serbia,
Precious Achiuwa,Nigeria,2020,Nigeria,
R.J. Barrett,Canada,2019,Canada,
Raul Neto,Brazil,2015,Brazil,
Robert Archibald,United Kingdom,2002,United Kingdom,
Rodions Kurucs,Latvia,2018,Latvia,
Ruben Boumtje-Boumtje,Cameroon,2001,Cameroon,
Rudy Fernández,Spain,2008,Spain,
Rudy Gobert,France,2013,France,
Ryan Broekhoff,Australia,2018,Australia,
Santi Aldama,Spain,2021,Spain,
Scott Machado,Brazil,2012,Brazil,
Sean Marks,New Zealand,1998,New Zealand,
Sergio Llull,Spain,2009,Spain,
Sergio Rodríguez,Spain,2006,Spain,
Sergey Karasev,Russia,2013,Russia,
Shai Gilgeous-Alexander,Canada,2018,Canada,
Sim Bhullar,Canada,2015,Canada,
Solomon Alabi,Nigeria,2010,Nigeria,
Sviatoslav Mykhailiuk,Ukraine,2018,Ukraine,
Thabo Sefolosha,Switzerland,2006,Switzerland,
Theo Maledon,France,2020,France,
Thon Maker,Australia,2016,Australia,
Timothé Luwawu-Cabarrot,France,2016,France,
Tomas Satoransky,Czech Republic,2016,Czech Republic,
Tristan Thompson,Canada,2011,Canada,
Tyler Ennis,Canada,2014,Canada,
Uroš Plavšić,Serbia,2024,Serbia,
Usman Garuba,Spain,2021,Spain,
Vasilis Spanoulis,Greece,2006,Greece,
Victor Claver,Spain,2012,Spain,
Victor Wembanyama,France,2023,France,
Viktor Khryapa,Russia,2004,Russia,
Vít Krejčí,Czech Republic,2021,Czech Republic,
Vitaly Potapenko,Ukraine,1996,Ukraine,
Vladimir Radmanović,Serbia,2001,Serbia,
Vladimir Stepania,Georgia,1998,Georgia,
Walter Herrmann,Argentina,2006,Argentina,
Wang Zhizhi,China,2001,China,
Willy Hernangómez,Spain,2016,Spain,
Yao Ming,China,2002,China,
Yuta Tabuse,Japan,2004,Japan,
Yuta Watanabe,Japan,2018,Japan,
Zhou Qi,China,2017,China,
Zoran Dragić,Slovenia,2014,Slovenia,
Zoran Planinić,Croatia,2003,Croatia
"""

intl_df = pd.read_csv(StringIO(intl_players_csv.strip()))

# nombres
intl_df['player_name'] = intl_df['player_name'].str.lower().str.strip()
df['player_norm'] = df['player'].str.lower().str.strip()

# país de nacimiento
df = df.merge(
    intl_df[['player_name', 'birth_country', 'nationality']],
    left_on='player_norm',
    right_on='player_name',
    how='left'
)
df.drop(columns=['player_name', 'player_norm'], inplace=True)

# internacional
df['international'] = df['birth_country'].notna() & (df['birth_country'] != 'United States')

# lugar de nacimeinto
continent_map = {
    'Spain': 'Europe', 'France': 'Europe', 'Serbia': 'Europe', 'Croatia': 'Europe', 'Lithuania': 'Europe',
    'Slovenia': 'Europe', 'Greece': 'Europe', 'Germany': 'Europe', 'Italy': 'Europe', 'Latvia': 'Europe',
    'Russia': 'Europe', 'Ukraine': 'Europe', 'Czech Republic': 'Europe', 'Turkey': 'Europe', 'Poland': 'Europe',
    'United Kingdom': 'Europe', 'Switzerland': 'Europe', 'Austria': 'Europe', 'Bosnia and Herzegovina': 'Europe',
    'Montenegro': 'Europe', 'Georgia': 'Europe',
    'Canada': 'North America', 'Dominican Republic': 'North America', 'Puerto Rico': 'North America',
    'Mexico': 'North America', 'Bahamas': 'North America',
    'Brazil': 'South America', 'Argentina': 'South America', 'Colombia': 'South America',
    'Australia': 'Oceania', 'New Zealand': 'Oceania',
    'Nigeria': 'Africa', 'Senegal': 'Africa', 'Cameroon': 'Africa', 'Angola': 'Africa', 'South Sudan': 'Africa',
    'Democratic Republic of the Congo': 'Africa', 'Cape Verde': 'Africa',
    'China': 'Asia', 'Japan': 'Asia', 'South Korea': 'Asia'
}
df['continent'] = df['birth_country'].map(continent_map).fillna('North America')

# stats
df = df.merge(
    data['player_totals'][['season', 'player_id', 'team', 'mp', 'pts', 'trb', 'ast']],
    on=['season', 'player_id', 'team'],
    how='left'
)

df = df.merge(
    data['player_per100'][['season', 'player_id', 'team', 'pts_per_100_poss', 'o_rtg', 'd_rtg']],
    on=['season', 'player_id', 'team'],
    how='left'
)

# Premios
df['all_star'] = df[['season', 'player_id']].merge(
    data['all_star'][['season', 'player_id']].assign(all_star=True),
    on=['season', 'player_id'], how='left'
)['all_star'].fillna(False)

df['all_nba'] = df[['season', 'player_id']].merge(
    data['end_teams'][data['end_teams']['type'].str.contains('All-NBA', na=False)][['season', 'player_id']].assign(all_nba=True),
    on=['season', 'player_id'], how='left'
)['all_nba'].fillna(False)

# Calculo variables
team_group = df.groupby(['season', 'team'])

df['team_total_mp'] = team_group['mp'].transform('sum')
df['intl_minutes'] = np.where(df['international'], df['mp'].fillna(0), 0)
df['team_intl_minutes'] = team_group['intl_minutes'].transform('sum')
df['pct_intl_minutes_team'] = df['team_intl_minutes'] / df['team_total_mp'].replace(0, np.nan)

# Impacto
df['player_impact'] = df['pts_per_100_poss'].fillna(0) * (df['mp'].fillna(0) / df['team_total_mp'].replace(0, np.nan))
df['intl_impact_index'] = team_group['player_impact'].transform(lambda x: x[df['international']].sum())

# Playoffs
playoff_teams = data['team_abbrev'][data['team_abbrev']['playoffs'] == True][['season', 'abbreviation']]
df = df.merge(playoff_teams, left_on=['season', 'team'], right_on=['season', 'abbreviation'], how='left', indicator=True)
df['in_playoffs'] = df['_merge'] == 'both'
df.drop(columns=['_merge', 'abbreviation'], inplace=True)

playoff_group = df[df['in_playoffs']].groupby(['season', 'team'])
df['team_intl_minutes_playoffs'] = playoff_group['intl_minutes'].transform('sum')
df['team_total_mp_playoffs'] = playoff_group['mp'].transform('sum')
df['pct_intl_minutes_team_playoffs'] = df['team_intl_minutes_playoffs'] / df['team_total_mp_playoffs'].replace(0, np.nan)
df['playoff_protagonism'] = df['pct_intl_minutes_team_playoffs'] / df['pct_intl_minutes_team'].replace(0, np.nan)

# Campeon
champions = data['team_summaries'].loc[data['team_summaries'].groupby('season')['w'].idxmax()][['season', 'abbreviation']]
df = df.merge(champions, left_on=['season', 'team'], right_on=['season', 'abbreviation'], how='left', indicator=True)
df['champion'] = df['_merge'] == 'both'
df.drop(columns=['_merge', 'abbreviation'], inplace=True)

# Limpieza
df.fillna({
    'mp': 0, 'pts': 0, 'pts_per_100_poss': 0, 'o_rtg': 0, 'd_rtg': 0,
    'pct_intl_minutes_team': 0, 'intl_impact_index': 0,
    'pct_intl_minutes_team_playoffs': 0, 'playoff_protagonism': 0,
    'birth_country': 'USA', 'continent': 'North America'
}, inplace=True)

print(f"\nDataset final: {df.shape[0]} filas, {df.shape[1]} columnas")
print("Columnas:", df.columns.tolist())
print("\nEjemplo:")
print(df[['season', 'player', 'team', 'international', 'birth_country', 'continent', 
          'pct_intl_minutes_team', 'intl_impact_index', 'playoff_protagonism', 'champion']].head(10))

df.to_csv('nba_globalizacion_2001_2026_FINAL.csv', index=False)
print("\n¡Exportado como 'nba_globalizacion_2001_2026_FINAL.csv'! Listo para la Parte II.")