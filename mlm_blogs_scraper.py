import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import arrow


def get_soup(link: str) -> bs:
    return bs(requests.get(link).text)


def get_blog_info(soup) -> pd.DataFrame:
    items = []
    for item in soup.select("#main article"):

        blog = {
            "Title": (title_node := item.select_one(".title > a")).text,
            "Date": arrow.get(item.select_one(".date")["title"]).datetime,
            "Short Summary": item.select_one(".entry > p").text.strip(),
            "Link": title_node["href"],
        }
        items.append(blog)
    return pd.DataFrame(items)


def main():
    link = "https://machinelearningmastery.com/blog/"
    out_folder = "C:/Users/quent/OneDrive/Desktop/_L7_AI/Reading"
    results = []

    while True:
        soup = get_soup(link)
        page = int(soup.select_one(".current").text)
        df = get_blog_info(soup)
        results.append(df)

        if soup.select_one(".next") is None:
            break  # last page
        link = f"https://machinelearningmastery.com/blog/page/{page+1}/"

    blogs = pd.concat(results)
    blogs['is_sponsored_post'] = blogs['Short Summary'].str.contains(
        'Sponsored Post') * 1
    # blogs.sort_values('Date', ascending = False, inplace=True)
    blogs.to_csv(f"{out_folder}/mlm_blogs.csv",
                 index=False, encoding="utf-8-sig")


if __name__ == "__main__":

    try:
        main()

    except KeyboardInterrupt:
        exit("Bye!")
