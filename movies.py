def get_films_data(start_date, end_date):
    """Build a dataframe of information about movies between two dates."""
    import movies

    films = movies.list_of_films(start_date, end_date)
    films_list = movies.get_film_list_details(films)
    df = movies.build_films_df(films_list)

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

    import pandas as pd
    import requests
    import config
    api_key = config.api_key
    from tqdm import tqdm_notebook as tqdm
    # tqdm().pandas()

    query_string = 'https://api.themoviedb.org/3/discover/movie?api_key=' \
                    + api_key \
                    + '&primary_release_date.gte=' + start_date \
                    + '&primary_release_date.lte=' + end_date \
                    + '&include_adult=false' \
                    + '&with_release_type=3&region=US' \
                    + '&sort_by=vote_average.desc' \
                    + '&vote_count.gte=50' \
                    + '&without_genres=99|10770'

    pages = requests.get(query_string).json()['total_pages']

    films_list = []

    for page in tqdm(range(1, pages+1)):
        response = requests.get(query_string + '&page={}'.format(page))
        films = response.json()['results']
        films_list.extend(films)

    return films_list


def get_film_details(films):
    """Query TMDb for details on a list of movies."""

    import requests
    import config
    api_key = config.api_key
    from tqdm import tqdm_notebook as tqdm

    films_list = []

    for film in tqdm(films):
        entry = requests.get('https://api.themoviedb.org/3/movie/'
                               + str(film['id'])
                               + '?api_key=' + api_key
                               + '&language=en-US'
                               + '&append_to_response=credits'
                            )
        entry = (entry.json())
        films_list += [entry]

    return films_list


def get_film_list_details(films):
    """Break a long list of films into smaller chunks and pass each
    smaller list to get_film_details.

    This process avoids querying a list of thousands of films, which
    creates problems and tends to break.
    """

    from tqdm import tqdm_notebook as tqdm

    idchunks = [films[x:x + 250] for x in range(0, len(films), 250)]

    filmslist = []
    for ids in tqdm(idchunks):
        results = get_film_details(ids)
        filmslist.extend(results)

    return filmslist


def bin_budget(df):
    """Bin budgets into different buckets."""

    import pandas as pd

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

    import pandas as pd
    import cpi
    import movies

    df = pd.DataFrame(films_list) \
        .drop(columns=['adult', 'backdrop_path', 'imdb_id', 'homepage',
                       'overview', 'poster_path', 'tagline', 'video',
                       'belongs_to_collection', 'original_title'])

    df['release_date'] = pd.to_datetime(df['release_date'])

    df['year'] = df['release_date'].dt.year

    df['decade'] = ((df.year)//10)*10

    df['budget_adj'] = df.apply(lambda x: cpi.inflate(x['budget'], x['year'])
        if (x['year'] !=2019) else x['budget'], axis=1)

    df['revenue_adj'] = df.apply(lambda x: cpi.inflate(x['revenue'], x['year'])
        if (x['year'] !=2019) else x['revenue'], axis=1)

    df['profit'] = df['revenue'] - df['budget']

    df['profit_adj'] = df['revenue_adj'] - df['budget_adj']

    df = movies.bin_budget(df)

    return df


def rankings(director_df, director=False, actor=False):
    """Determine rankings for directors.

    Calculate the total films, profit and budgets for directors, their average
    profit per film, and then their rankings for each of those categories.
    """

    import pandas as pd

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

    import pandas as pd

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

    import pandas as pd

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
