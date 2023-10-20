import bs4 as bs
import requests
import pickle
def save_sp500_tickers():
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker[:-1])
    return tickers

tickers = save_sp500_tickers()

with open('tickers.pkl', 'wb') as f:  # open a text file
    pickle.dump(tickers, f) # serialize the list
    f.close()

