'''
 Requirements in addition to below library imports
 1. bs4 version 4.7.1+ for soupsieve functionality
 2. lxml installed as bs4 parser. Or change the parser e.g. 'html.parser'
 
'''

# USER MODIFIED INPUTS #################################################################################################

# User modifies
import pandas as pd
import httpx
import trio
from bs4 import BeautifulSoup as bs
import requests
import re
API_CLASS_OUTPUT_CSV = 'C:/Users/quent/OneDrive/Desktop/scikitlearn_api_class_info.csv'

API_CLASS_PARAM_OUTPUT_CSV = 'C:/Users/quent/OneDrive/Desktop/scikitlearn_api_class_param_info.csv'  # User modifies

########################################################################################################################


API_REF_LINK = ['https://scikit-learn.org/stable/modules/classes.html']

DFS = []


def get_api_classes_info() -> pd.DataFrame:
    """
    Param:
    None

    Example call:
    df = get_api_classes_info()

    Returns:
            DataFrame of the scikit classes
    """

    soup = bs(
        requests.get(
            "https://scikit-learn.org/stable/modules/classes.html").text,
        "lxml",
    )
    ref_tables = []

    for s in soup.select("section#api-reference h2:not(h2:has(+[id^=to-be-removed]))"):

        current_h2 = s.text
        next_h2 = s.find_next("h2").text
        a = "deprecated" in next_h2
        # print(current_h2, next_h2)

        section = s.find_previous("section")
        section_title = section["id"]
        section_text = s.get_text(" ").replace(" ¶", "")

        library, sk_class = re.split(r"\s+:\s+", section_text, maxsplit=2)
        # print((library, sk_class))

        for t in section.select("table:has(tr)"):
            t1 = pd.read_html(str(t))[0]
            t1_links = [
                "https://scikit-learn.org/stable/modules/"
                + i.select_one("a.reference")["href"]
                for i in t.select("td:nth-of-type(1)")
            ]
            t1["link"] = t1_links
            t1["section_title"] = section_title
            t1["library"] = library
            t1["sk_class"] = sk_class
            t1["section_text"] = section_text
            ref_tables.append(t1)
            # print(t1.to_markdown)

        if a:
            # last block
            break

    ref_df = pd.concat(ref_tables, ignore_index=True)  # , columns = []
    ref_df.columns = ["class", "class description"] + list(ref_df.columns[2:])

    return ref_df


def get_param_info(soup) -> pd.DataFrame:   # bs4 object type?
    """
    Param:
    soup - parsed html from model url - url of sciki-learn model documentation

    Returns:
            DataFrame of the model parameter info which can then be used with
            e.g. grid_values to input into GridSearchCV | RandomizedSearchCV
    """

    model = soup.select_one(
        "h1:has(.reference), h1:has(.headerlink)").text.replace("¶", "")

    results = []

    for i in soup.select(
        '.field-list:not(.simple) dt:-soup-contains("Parameters") + dd dt'
    ):  # requires bs4 4.7.1+

        matches = []
        param = ''
        option_text = ''
        description = ''
        options = []
        data_types = ''
        default = ''

        try:
            param = i.select_one("strong").text
        except:
            continue

        if ': ' in param:

            param, option_text = param.split(': ')

        else:

            option_text = re.sub(
                r"\{(.*?'?)\}(.*)",
                r"[\1]\2",
                re.sub(r"[“‘’”]", "'", i.select_one(".classifier").text),
            )

        option_text = re.sub(r"[\s\n\t]+", " ", option_text)
        is_options_without_braces = re.match(r"(^'.*?',.*?),", option_text)

        description_node = i.find_next("dd").p

        if description_node:
            description = description_node.get_text(" ")

        if 'default' in option_text:
            default = re.search(r"default=?'?(.*?)'?$",
                                option_text).groups(1)[0]

        if "[" in option_text:

            options = re.search(r"(\[.*?\])", option_text).groups(1)[0]
            matches = [
                i
                for i in re.split(r"^(\[.*?\],\s+)(.*?)(?=,\s+default).*$", option_text)
            ][1:-1]

            if len(matches) >= 2:
                data_types = matches[1]
            else:
                data_types = ""

        elif is_options_without_braces:

            options = (
                "["
                + re.sub(
                    r"\s+,",
                    ",",
                    re.sub(r"\bor\b", ",", is_options_without_braces.groups(
                        0)[0], flags=re.I),
                )
                + "]"
            )

        else:
            data_types = re.sub(r"^(.*),\s+default.*", r"\1", option_text)
            options = ""

        results.append(
            [model, param, options, data_types, default, description])

    if results:

        df = pd.DataFrame(
            results,
            columns=["model", "param", "options",
                     "data_types", "default", "description"]
        )

    else:

        description_node = soup.select_one('h1 + dl p, h1 + p')

        if description_node:
            description = description_node.get_text(" ")

        df = pd.DataFrame(
            [[model, '', '', '', '', description]],
            columns=["model", "param", "options",
                     "data_types", "default", "description"]
        )

    return df


async def get_soup(content):
    return BeautifulSoup(content, 'lxml')


async def get_param_info(url, nurse):
    async with httpx.AsyncClient(http2=True, timeout=None) as client:
        r = await client.get(url)
        try:
            # modify func to use soup rather than url
            DFS.append(get_param_info(soup))
        except Exception as e:
            print(f'Failed link {url} with exception: {e}')
            print()


async def main():

    api_class_info = get_api_classes_info()
    api_links = list(api_class_info["link"].values)
    # write out initial api classes info
    api_class_info.to_csv(API_CLASS_OUTPUT_CSV,
                          index=False, encoding='utf-8-sig')

    async with trio.open_nursery() as nurse:
        for link in api_links:
            # get indiv api param info
            nurse.start_soon(get_param_info, link, nurse)

    params_df = pd.concat(DFS, ignore_index=True)
    params_df.to_csv(API_CLASS_PARAM_OUTPUT_CSV,
                     encoding='utf-8-sig', index=False)

if __name__ == "__main__":

    try:
        trio.run(main)

    except KeyboardInterrupt:
        exit('Bye!')
