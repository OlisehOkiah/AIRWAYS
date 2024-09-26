import time
from random import uniform
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
import requests
from bs4 import BeautifulSoup
import re


# Define custom scraper class
class ScrapeSkyTrax:
    def __init__(self, link: str):
        self.link = link
        self.pattern = re.compile(r"comp comp_media-review-rated list-item media position-content review-\d+")

    def scrape(self, session):
        r = session.get(self.link)

        # Parse the page content using BeautifulSoup
        soup = BeautifulSoup(r.content, "lxml")
        article_parent = soup.find("article", class_="comp comp_reviews-airline querylist position-content")

        if article_parent:
            articles = article_parent.find_all("article", class_=self.pattern)
            dates = article_parent.find_all('time')
            ratings = article_parent.find_all('div', class_="rating-10")
            countries = article_parent.find_all('h3', class_="text_sub_header")
            return articles, dates, ratings, countries
        else:
            print(f"Warning: No review articles found on page {self.link}")
            return [], [], [], []


# Function to fetch a single page with error handling and delay (rate limiting)
def fetch_page(page_num: int, session):
    url = f"https://www.airlinequality.com/airline-reviews/british-airways/page/{page_num}/"
    scraper = ScrapeSkyTrax(url)

    # Simulate a delay to avoid overwhelming the server (rate limiting)
    delay = uniform(1, 3)
    time.sleep(delay)

    # Try scraping the page and return the articles, dates, ratings, and countries
    try:
        return scraper.scrape(session)
    except Exception as e:
        print(f"Error while scraping page {page_num}: {e}")
        return [], [], [], []


# Function to fetch reviews concurrently with error handling and rate limiting
def fetch_reviews_concurrently(max_pages: int) -> List[Dict[str, str]]:
    my_reviews = []
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(fetch_page, i, session) for i in range(1, max_pages)]
            for future in as_completed(futures):
                try:
                    articles, dates, ratings, countries = future.result()  # Lists of articles, dates, ratings, countries
                    if articles:
                        for i, article in enumerate(articles):
                            review = {}

                            # Extract the review text
                            review_div = article.find('div', class_='text_content')
                            review_text = review_div.get_text().strip() if review_div else "N/A"
                            review['review'] = review_text

                            # Extract the date
                            review_date = dates[i]['datetime'] if i < len(dates) else "N/A"
                            review['date'] = review_date

                            # Extract the rating
                            rating_span = ratings[i].find('span') if i < len(ratings) else None
                            review_rating = rating_span.text if rating_span else "N/A"
                            review['rating'] = review_rating

                            # Extract the country
                            country_span = countries[i].span if i < len(countries) else None
                            review_country = country_span.next_sibling.text.strip(" ()") if country_span else "N/A"
                            review['country'] = review_country

                            my_reviews.append(review)
                except Exception as e:
                    print(f"Error in processing future result: {e}")
    return my_reviews