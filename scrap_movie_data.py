import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_imdb(movie_id):
    url = f"https://www.imdb.com/title/{movie_id}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')

        poster_url = ''
        poster_tag = soup.select_one('div[data-testid="hero-media__poster"] img')
        if poster_tag and poster_tag.has_attr('src'):
            poster_url = poster_tag['src']

        # Genre (rounded boxes below poster/trailer)
        genres = [g.text.strip() for g in soup.select('div.ipc-chip-list--baseAlt span.ipc-chip__text')]
        genres = ', '.join(genres)

        # Top 5 actors (under "Top cast")
        actors = [a.text.strip() for a in soup.select('a[data-testid="title-cast-item__actor"]')]
        actors = ', '.join(actors[:5])

        # Director(s)
        director = ''
        director_section = soup.find('li', attrs={'data-testid': 'title-pc-principal-credit'})
        if director_section:
            director_links = director_section.select('a')
            director = ', '.join([a.text.strip() for a in director_links])

        # Number of votes (right below IMDb rating, handles K/M abbreviations)
        votes = ''
        votes_tag = soup.find('span', {'data-testid': 'rating-histogram-vote-count'})
        if votes_tag:
            votes_text = votes_tag.get_text(strip=True).replace(',', '')
            if 'K' in votes_text:
                votes = str(int(float(votes_text.replace('K', '')) * 1000))
            elif 'M' in votes_text:
                votes = str(int(float(votes_text.replace('M', '')) * 1000000))
            else:
                votes = votes_text
                
        return {
            'movie_id': movie_id,
            'genre': genres,
            'votes': votes,
            'top_5_actors': actors,
            'director': director,
            'poster_url': poster_url
        }
    except Exception as e:
        return {
            'movie_id': movie_id,
            'genre': '',
            'votes': '',
            'top_5_actors': '',
            'director': '',
            'poster_url': ''
        }

if __name__ == "__main__":
    df = pd.read_csv('movie_data.csv')

    # Add columns if they don't exist
    for col in ['genre', 'votes', 'top_5_actors', 'director', 'poster_url']:
        if col not in df.columns:
            df[col] = ''

    # Take user input for batch scraping
    start_index = int(input("Enter starting index (0-based): "))
    batch_size = int(input("Enter number of movies to scrape: "))

    # Select the subset to process
    subset = df.iloc[start_index : start_index + batch_size]

    # Map movie_id to DataFrame index for correct updating
    movieid_to_index = {row['movie_id']: idx for idx, row in subset.iterrows()}

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_imdb, row['movie_id']): row['movie_id'] for _, row in subset.iterrows()}
        for future in as_completed(futures):
            result = future.result()
            idx = movieid_to_index.get(result['movie_id'])
            if idx is not None:
                for col in ['genre', 'votes', 'top_5_actors', 'director', 'poster_url']:
                    df.at[idx, col] = result[col]

    # Overwrite the original CSV with updated data
    df.to_csv('movie_data.csv', index=False)
