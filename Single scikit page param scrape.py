"""
Requirements in addition to below library imports

    1. bs4 version 4.7.1+ for soupsieve functionality
    2. lxml installed as bs4 parser. Or change the parser e.g. 'html.parser'

Package versions at time of writing:

    re == 2.2.1
    requests == 2.27.1
    bs4 == 0.0.1
    beautifulsoup4 == 4.11.1
    pandas == 1.4.2

"""

from bs4 import BeautifulSoup as bs
import time
import pandas as pd
import re
import requests

pd.set_option('display.width', 0)

# USER MODIFIED INPUTS #################################################################################################

API_CLASS_PARAM_OUTPUT_CSV = "<your_path>/scikitlearn_api_class_param_info.csv"

########################################################################################################################

DFS = []


def get_param_info(link: str) -> pd.DataFrame:

    soup = bs(requests.get(link).content, "lxml")
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

        # print(option_text)

        description_node = i.find_next("dd").p

        if description_node:
            description = description_node.text

        if 'default' in option_text:
            default = re.search(r"default=?'?(.*?)'?$",
                                option_text).groups(1)[0]

        if "[" in option_text:

            options = re.search(r"(\[.*?\])", option_text).groups(1)[0]
            matches = [
                i for i in re.split(r"^(\[.*?\],\s+)(.*?)(?=,\s+default).*$", option_text)
            ][1:-1]

            if len(matches) >= 2:
                data_types = matches[1]

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

            options = ''
            data_types = re.sub(r"^(.*),\s+default.*", r"\1", option_text)

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
            description = description_node.text

        df = pd.DataFrame(
            [[model, '', '', '', '', description]],
            columns=["model", "param", "options",
                     "data_types", "default", "description"]
        )

    return df


df = get_param_info(
    "https://scikit-learn.org/stable/modules/generated/sklearn.metrics.classification_report.html#sklearn.metrics.classification_report"
)

print(df)
