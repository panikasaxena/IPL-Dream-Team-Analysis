import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import asyncio
import aiohttp

from concurrent.futures import ThreadPoolExecutor


async def main():
    match_result_url = 'https://www.espncricinfo.com/records/year/team-match-results/2023-2023/twenty20-matches-6'

    try:
        response = requests.get(match_result_url, timeout=10)
        response.raise_for_status()  # Ensure the request succeeded

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')  # Adjust this if needed to find specific tables

        if table:
            # Convert HTML table to DataFrame
            html_content = str(table)
            data = StringIO(html_content)
            df = pd.read_html(data)[0]

            match_urls = get_urls_from_column(table, 6)
            df['Match URLs'] = match_urls

            df = df[df.iloc[:, 6] != 'Twenty20']
            #print("Number of rows:", df.shape[0])
            #print("Number of columns:", df.shape[1])
            save_table_to_csv(df, 'match_results')
            url_df = pd.read_csv('match_results.csv')
            # common_df = shared_data(df)
            urls = df['Match URLs']
            await get_bowling_batting_summary(url_df)
        else:
            print("No tables found at the URL.")

    except Exception as e:
        print(f"An error occurred: {e}")


def shared_data(df) -> pd:
    # df = pd.read_html(data)[0]  # Reads the first table into a DataFrame

    # Creating a new DataFrame with combined columns
    new_df = pd.DataFrame({
        'Match': df.iloc[:, 0] + ' VS ' + df.iloc[:, 1],  # Use .iloc to access columns by index
        'MatchID': df.iloc[:, 6]  # Assumes the 7th column is the 'MatchID'
    })
    num_rows = df.shape[0]
    #print("Number of rows:", num_rows)

    return new_df


def save_table_to_csv(df, csv_filename):
    # Save the DataFrame to a CSV file
    df.to_csv(csv_filename + '.csv', index=False)
    print(f"Table saved as '{csv_filename}.csv'.")


def get_urls_from_column(table, column: int) -> list[str]:
    links = []
    rows = table.find_all('tr')
    # print(rows)
    for row in rows:
        cells = row.find_all('td')
        # print(cells)
        if cells:  # Ensure it's not a header
            link_tag = cells[column].find('a', href=True)
            # print(link_tag)
            if link_tag:  # Ensure there is an <a> tag
                full_url = link_tag['href']
                if not full_url.startswith('http'):
                    full_url = 'https://www.espncricinfo.com' + full_url  # Adjust the base URL as necessary
                links.append(full_url)

    #print(len(links))
    return links


async def fetch_data(session, url):
    try:
        async with session.get(url, timeout=1) as response:
            response.raise_for_status()  # Ensures the request succeeded
            return await response.text()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


async def process_url(session, url, match, match_id):
    try:
        html = await fetch_data(session, url)
        if html:
            soup = BeautifulSoup(html, 'html.parser')
            tables = soup.find_all('table')
            batting_data = pd.DataFrame()
            bowling_data = pd.DataFrame()
            for index, table in enumerate(tables):
                if index == 0 or index == 2:
                    new_bat = batting_tables(table)  # You need to define this function
                    new_bat['Match'] = match
                    new_bat['Match ID'] = match_id
                    batting_data = pd.concat([batting_data, new_bat], ignore_index=True)
                elif index == 1 or index == 3:
                    new_bowl = bowling_tables(table)  # You need to define this function
                    new_bowl['Match'] = match
                    new_bowl['Match ID'] = match_id
                    bowling_data = pd.concat([bowling_data, new_bowl], ignore_index=True)
            return batting_data, bowling_data
        else:
            return pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        print(f"Error processing URL {url}: {e}")
        return pd.DataFrame(), pd.DataFrame()


async def get_bowling_batting_summary(df):
    #print('in')
    #print(df)
    batting_pd = pd.DataFrame()
    bowling_pd = pd.DataFrame()
    match_urls = df['Match URLs']
    #print(len(match_urls))

    common = pd.DataFrame({
        'Match': df.iloc[:, 0] + ' VS ' + df.iloc[:, 1],  # Use .iloc to access columns by index
        'MatchID': df.iloc[:, 6]  # Assumes the 7th column is the 'MatchID'
    })
    #print(df.shape[0])

    async with aiohttp.ClientSession() as session:
       # print('i')
        # Use await inside list comprehension
        tasks = [await process_url(session, match_urls[i], common.iloc[i, 0], common.iloc[i, 1]) for i in
                 range(len(match_urls))]
        #print('j')
        #results = await asyncio.gather(*tasks)  # Await all tasks here
        #print(type(tasks[0]))

        for url_data in tasks:
            batting_data = url_data[0]
            bowling_data = url_data[1]
            if not batting_data.empty and not bowling_data.empty:  # Check if dataframes are not empty
                batting_pd = pd.concat([batting_pd, batting_data], ignore_index=True)
                bowling_pd = pd.concat([bowling_pd, bowling_data], ignore_index=True)
            else:
                print("Skipping empty DataFrame.")
    save_table_to_csv(batting_pd,'batting_summary')
    save_table_to_csv(bowling_pd,'bowling_summary')


def batting_tables(table) -> pd.DataFrame:
    html_content = str(table)
    data = StringIO(html_content)
    dfs = pd.read_html(data)
    if dfs:  # Check if dfs is not empty
        df = dfs[0]
        #df.reset_index(drop=True, inplace=True)
        #df.index = df.index + 1
        #df['Index'] = df.index
    else:
        df = pd.DataFrame()  # Return an empty DataFrame
    return df

def bowling_tables(table) -> pd.DataFrame:
    html_content = str(table)
    data = StringIO(html_content)
    dfs = pd.read_html(data)
    if dfs:

        # Check if dfs is not empty
        df = dfs[0]
       # print('error not here')
    else:
        df = pd.DataFrame()  # Return an empty DataFrame
    return df


# def get_player_urls():
#
# def get_player_summary():


if __name__ == "__main__":
    asyncio.run(main())
