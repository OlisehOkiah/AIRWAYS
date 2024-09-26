import pandas as pd
import time
from typing import List, Dict
from main import fetch_reviews_concurrently

# Loading the Data and Cleaning it

# Set the max pages to scrape
max_pages = 387

# Measure the time taken for the operation
t1 = time.perf_counter()
# Fetch the reviews concurrently
all_reviews = fetch_reviews_concurrently(max_pages)

t2 = time.perf_counter()

print(f'Finished in {t2 - t1} seconds')

# Print the number of reviews scraped
print(f"Total number of reviews scraped: {len(all_reviews)}")


def save_into_csv(my_list: List[Dict[str, str]]):
    df = pd.DataFrame(my_list)
    df.to_csv('reviews.csv', index=False)


save_into_csv(all_reviews)

df=pd.read_csv('reviews.csv')
df.head()