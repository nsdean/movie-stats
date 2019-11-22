from tqdm import tqdm_notebook as tqdm
import requests
import pandas as pd
import config
tmdb_key = config.tmdb_key
omdb_key = config.omdb_key
from bs4 import BeautifulSoup
import cpi

def get_films_data(start_date, end_date):
    """Build a dataframe of information about movies between two dates."""

    films = list_of_films(start_date, end_date)
    films_list = get_film_list_details(films)
    df = build_films_df(films_list)
    print('Done.')

    return df


def list_of_films(start_date, end_date):
    """Query TMDb for movies between two dates.

    Will run requests of TMDb API for movies with US theatrical release dates
    between specified dates.  Dates should be given in YYYY-MM-DD format.

    Filter out adult movies.  Filter movies that received fewer than 50 votes
    to try and get more populat releases.  Results will be ordered by average
    voter score.  Also filter out documentaries, as we're only interested in
    feature films.

    First pull number of pages.  Then cycle through all pages and retrieve
    full data.

    Returns a list of dictionaries.  Each dictionary is one film.

    Note that '&with_release_type=3&region=US' selects only US theatrical
    releases, '&vote_count.gte=50' filters films with fewer than 50 votes,
    and '&without_genres=99|10770' filters out documentaries and TV movies.
    """

    query_string = 'https://api.themoviedb.org/3/discover/movie?api_key=' \
                    + tmdb_key \
                    + '&primary_release_date.gte=' + start_date \
                    + '&primary_release_date.lte=' + end_date \
                    + '&include_adult=false' \
                    + '&with_release_type=3&region=US' \
                    + '&sort_by=vote_average.desc' \
                    + '&vote_count.gte=50' \
                    + '&without_genres=99|10770'

    pages = requests.get(query_string).json()['total_pages']

    films_list = []

    print('Get list of films.')
    for page in tqdm(range(1, pages+1)):
        response = requests.get(query_string + '&page={}'.format(page))
        films = response.json()['results']
        films_list.extend(films)

    return films_list


def get_film_details(films):
    """Query TMDb for details on a list of movies."""

    films_list = []

    for film in tqdm(films):
        try:
            entry = requests.get('https://api.themoviedb.org/3/movie/'
                                   + str(film['id'])
                                   + '?api_key=' + tmdb_key
                                   + '&language=en-US'
                                   + '&append_to_response=credits'
                                )
            entry = (entry.json())
            films_list += [entry]
        except:
            print('Couldn\'t get film ' + str(film['id']))
            continue

    return films_list


def get_film_list_details(films):
    """Break a long list of films into smaller chunks and pass each
    smaller list to get_film_details.

    This process avoids querying a list of thousands of films, which
    creates problems and tends to break.
    """

    print('Get details for each film.')

    idchunks = [films[x:x + 250] for x in range(0, len(films), 250)]

    filmslist = []
    for ids in tqdm(idchunks):
        results = get_film_details(ids)
        filmslist.extend(results)

    return filmslist


def get_imdb_data(film_df):
    """Scrape IMBd for budget and revenue data for specific films.

    Performs a scrape of IMDb film pages for their budget and revenue details.
    Expected input is a dataframe of those films we don't already have this
    data on, but in principle it doesn't matter.
    """

    budget_list = []
    revenue_list = []
    id_list = []

    idchunks = [film_df[x:x + 250] for x in range(0, len(film_df), 250)]

    print('Getting results from IMDb...')

    for ids in tqdm(idchunks):
        for id_ in tqdm(ids['imdb_id']):
            url = 'https://www.imdb.com/title/' + id_
            id_list.append(id_)

            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')

            text_box = soup.find_all('div', class_='txt-block')

            if (soup.find(text='Budget:') is None):
                budget_list.append(0)

            if (soup.find(text='Cumulative Worldwide Gross:') is None):
                revenue_list.append(0)

            for t in text_box:
                try:
                    heading = t.find('h4').getText()
                    if heading == 'Budget:':
                        t1 = t.find('h4').next_sibling
                        t2 = t1.rstrip().lstrip('$')
                        t3 = int(t2.replace(',',''))
                        budget_list.append(t3)
                    if heading == 'Cumulative Worldwide Gross:':
                        r1 = t.find('h4').next_sibling
                        r2 = r1.strip().lstrip('$')
                        r3 = int(r2.replace(',',''))
                        revenue_list.append(r3)

                except:
                    continue

    imdb_data = pd.DataFrame(list(zip(id_list, budget_list, revenue_list)),
                         columns=['imdb_id', 'budget_imdb', 'revenue_imdb'])
    return imdb_data


def bin_budget(df):
    """Bin budgets into different buckets."""

    # bins = [0, 2000000, 5000000, 10000000, 30000000,
    #         50000000, 100000000, 250000000, 300000000]
    #
    # labels = ['0-2M', '2-5M', '5-10M', '10-30M',
    #           '30-50M', '50-100M', '100-250M', '250-300M']

    bins = [1, 2000000, 5000000, 10000000, 30000000, 50000000, 100000000,
            150000000, 250000000, 300000000, 600000000]

    labels = ['<2M', '2-5M', '5-10M', '10-30M', '30-50M', '50-100M', '100-150M',
              '150-250M', '250-300M', '>300M']

    df['budget_bin'] = pd.cut(df['budget'], bins, labels=labels)
    df['budget_adj_bin'] = pd.cut(df['budget_adj'], bins, labels=labels)

    return df


def build_films_df(films_list):
    """Build a dataframe from the list of TMDb API query results.

    The dataframe will add columns for release year and decade, adjust
    budgets and revenues for inflation, and bin budgets into buckets.
    """

    df = pd.DataFrame(films_list) \
        .drop(columns=['adult', 'backdrop_path', 'homepage',
                       'overview', 'poster_path', 'tagline', 'video',
                       'belongs_to_collection', 'original_title'])

    print('Get missing budget and revenue data from IMDb.')
    no_budget_revenue = df[(df['budget']==0) | (df['revenue']==0)].reset_index(drop=True)

    imdb_data = get_imdb_data(no_budget_revenue)

    df = df.merge(imdb_data, on='imdb_id', how='left')

    print('Fill in missing budget and revenue data with IMDb data.')
    df['budget'].replace(0, df['budget_imdb'], inplace=True)
    df['revenue'].replace(0, df['revenue_imdb'], inplace=True)
    df['budget'].fillna(0, inplace=True)

    print('Get release date information.')
    df['release_date'] = pd.to_datetime(df['release_date'])
    df['year'] = df['release_date'].dt.year
    df['decade'] = ((df.year)//10)*10

    print('Adjust financial data for inflation.')
    df['budget_adj'] = df.apply(lambda x: cpi.inflate(x['budget'], x['year'])
        if (x['year'] !=2019) else x['budget'], axis=1)

    df['revenue_adj'] = df.apply(lambda x: cpi.inflate(x['revenue'], x['year'])
        if (x['year'] !=2019) else x['revenue'], axis=1)

    df['profit'] = df['revenue'] - df['budget']

    df['profit_adj'] = df['revenue_adj'] - df['budget_adj']

    df = bin_budget(df)

    return df

def get_omdb_data(films):
    """Search OMDb to get Metacritic, Rotten Tomatoes and IMDb ratings for films."""

    omdb_key = config.omdb_key
    films_list = []
    missed = []
    bad_response = 0

    # Perform a query for each entry from TMDb.
    for film in tqdm(films['imdb_id']):
        entry = requests.get('http://omdbapi.com/?i=' + film +
                             '&apikey=' + omdb_key)

        if entry.status_code==200:
            f = entry.json()
            films_list += [f]
        else:
            bad_response +=1
            print('Couldn\'t get ' + 'http://omdbapi.com/?i=' + film + '&apikey=' + omdb_key)

    for i,a in enumerate(films_list):
        a['RT_score']=a['Metacritic_score']=a['IMdb_score']='NaN'
#         print(a)
        try:
            if len(a['Ratings'])==0:
                pass

# Iterate through the Ratings element, stored as a list of dictionaries #
            for b in a['Ratings']:
                if b['Source'] == 'Internet Movie Database':
                    a['IMdb_score']= float(b['Value'][:3])*10
                elif b['Source'] == 'Rotten Tomatoes':
                    a['RT_score']= float(b['Value'].split('%')[0])
                elif b['Source'] == 'Metacritic':
                    a['Metacritic_score'] = float(b['Value'].split('/')[0])
        except:
            continue

    return films_list


def rankings(director_df, director=False, actor=False):
    """Determine rankings for directors.

    Calculate the total films, profit and budgets for directors, their average
    profit per film, and then their rankings for each of those categories.
    """

    if director == True:
        role = 'director'
    if actor == True:
        role = 'actor'

    count = director_df.groupby([role])['title'].count()
    profit = director_df.groupby([role])['profit_adj'].sum()
    budget = director_df.groupby([role])['budget_adj'].sum()

    rank = pd.concat([count, profit, budget], axis=1, sort=False) \
            .reset_index() \
            .sort_values('title', ascending=False) \
            .rename(columns={'title': 'films', 'budget': 'total_budget'})

    rank['average_profit'] = rank['profit_adj'] / rank['films']
    rank['profit_rank'] = rank['profit_adj'].rank(method='max', ascending=False)
    rank['budget_rank'] = rank['budget_adj'].rank(method='max', ascending=False)
    rank['average_profit_rank'] = rank['average_profit'].rank(method='max', ascending=False)

    rank = rank.merge(director_df[['gender',role]].drop_duplicates(role),
                      on=role, how='left')

    return rank


def rankings_decades(director_df, director=False, actor=False):
    """Determine rankings for directors by decade.

    For each decade, calculate the total films, profit and budgets for directors, their average
    profit per film, and then their rankings for each of those categories.
    """

    if director == True:
        role = 'director'
    if actor == True:
        role = 'actor'

    count = director_df.groupby([role,'decade'])['title'].count()
    profit = director_df.groupby([role,'decade'])['profit_adj'].sum()
    budget = director_df.groupby([role,'decade'])['budget_adj'].sum()

    rank = pd.concat([count, profit, budget], axis=1, sort=False) \
            .reset_index() \
            .sort_values('title', ascending=False) \
            .rename(columns={'title': 'films', 'budget': 'total_budget'})

    rank['average_profit'] = rank['profit_adj'] / rank['films']
    rank['profit_rank'] = rank.groupby(['decade'])['profit_adj'].rank(method='max', ascending=False)
    rank['budget_rank'] = rank.groupby(['decade'])['budget_adj'].rank(method='max', ascending=False)
    rank['average_profit_rank'] = rank.groupby(['decade'])['average_profit'].rank(method='max', ascending=False)

    rank = rank.merge(director_df[['gender',role]].drop_duplicates(role),
                      on=role, how='left')

    return rank


def rankings_years(director_df, director=False, actor=False):
    """Determine rankings for directors by year.

    For each year, calculate the total films, profit and budgets for directors, their average
    profit per film, and then their rankings for each of those categories.
    """

    if director == True:
        role = 'director'
    if actor == True:
        role = 'actor'

    count = director_df.groupby([role,'year'])['title'].count()
    profit = director_df.groupby([role,'year'])['profit_adj'].sum()
    budget = director_df.groupby([role,'year'])['budget_adj'].sum()

    rank = pd.concat([count, profit, budget], axis=1, sort=False) \
            .reset_index() \
            .sort_values('title', ascending=False) \
            .rename(columns={'title': 'films', 'budget': 'total_budget'})

    rank['average_profit'] = rank['profit_adj'] / rank['films']
    rank['profit_rank'] = rank.groupby(['year'])['profit_adj'].rank(method='max', ascending=False)
    rank['budget_rank'] = rank.groupby(['year'])['budget_adj'].rank(method='max', ascending=False)
    rank['average_profit_rank'] = rank.groupby(['year'])['average_profit'].rank(method='max', ascending=False)

    rank = rank.merge(director_df[['gender',role]].drop_duplicates(role),
                      on=role, how='left')

    return rank
