"""Functions to create a collaborator dataset."""
# %%
import json
from copy import copy
from itertools import chain
from urllib.parse import quote, urlencode
from warnings import warn

import pandas as pd
import requests

BASE_URL = "https://api.elsevier.com/"
SC_QUERY = "content/search/scopus?query="

API_KEY = None
with open(".scopus_api", 'r', encoding="UTF8") as file:
    API_KEY = file.readline()

with open("data/research_leaders.json", "r", encoding="UTF8") as file:
    research_leaders = json.load(file)['people']

PARAMS = {
    "apiKey": API_KEY,
    'httpAccept': 'application/json'
}


def get_publications(
    au_id: str | None = None,
    params: dict[str, str] | None = None,
    url: str | None = None,
    parse: bool = True,
    extra_fields: dict | None = None
) -> list[dict]:
    """Get the publications for a given author.

    Args:
        au_id (str | None, optional): A Scopus author ID. Defaults to None.
        params (dict[str, str] | None, optional): A dictionary of Scopus API \
            query parameters. Defaults to None.
        url (str | None, optional): A url (with urlencoded parameters) to \
            request. Defaults to None.
        parse (bool): If True, returns the parsed query. Defaults \
            to True.
        extra_fields (dict | None, optional): Extra field for each \
            publication dict. Defaults to None.

    Returns:
        list[dict]: _description_
    """
    if url:
        req = requests.get(
            url
        )
    else:
        if not au_id or not params:
            raise ValueError(
                "Either ('au_id' and 'params') or 'url' must be provided."
            )
        query = quote(f"AU-ID({au_id})")
        req = requests.get(
            BASE_URL + SC_QUERY + query,
            params=params
        )

    if not req:
        raise RuntimeError("Connection failed. Status: " +
                           str(req.status_code) + ". Reason: " + req.reason)

    content = req.json()["search-results"]

    per_req = int(content['opensearch:itemsPerPage'])
    start = int(content['opensearch:startIndex'])
    total = int(content['opensearch:totalResults'])

    if (start + per_req) < total:
        next = [link["@href"] for link in content["link"]
                if link["@ref"] == 'next'][0]

        result = [content] + get_publications(url=next, parse=False)

        return _parse_publications(result, extra_fields) if parse else result

    return _parse_publications([content], extra_fields) if parse else [content]


def _parse_publications(
    response_list: list[dict],
    extra_fields: dict | None
) -> list[dict]:
    """Merge the JSON responses from the Scopus search and parse each \
    publication entry.

    Args:
        response_list (list[dict]): A list of responses form Scopus search.
        extra_fields (dict | None): dict of extra fields to add to each \
            publication dict

    Returns:
        list[dict]: A list of publication entries.
    """
    def filter_entry(entry: dict) -> dict:
        keep = {
            'citedby-count': 'citedby-count',
            # 'dc:creator': 'first_author',
            'dc:identifier': 'id',
            'link': 'links',
            'prism:aggregationType': 'type',
            'prism:coverDate': 'date',
            'prism:doi': 'doi',
            'prism:publicationName': 'journal_name',
            'subtypeDescription': 'subtype'
        }
        return {value: entry.get(key) for key, value in keep.items()}

    def parse_fields(entry: dict, extra_fields: dict | None) -> dict:
        if extra_fields:
            entry.update(extra_fields)
        entry['publication_id'] = entry['id'].split(':')[-1]
        entry['authors_link'] = next(map(
            lambda x: '&'.join([
                x.get('@href').strip(',affiliation'),  # remove affiliation
                urlencode(PARAMS)]),                    # add PARAMS
            filter(
                # get only author-affiliation query
                lambda x: x['@ref'] == 'author-affiliation', entry['links']
            )))
        del entry['id'], entry['links']
        return entry

    merge = []
    for response in response_list:
        merge += copy(response["entry"])

    return [parse_fields(filter_entry(entry), extra_fields) for entry in merge]


def get_authors(
    url: str,
    pub_id: str | None = None,
    au_id: str | None = None
) -> list[dict]:
    """Get a list of dictionaries descrbing each coauthor.

    Keys include:
        'given_name', 'surname', 'author_order', 'scopus_id','affiliation_ids'

    Args:
        url (str): Scopus publication -> authors query url, provided by \
                   `get_publications`.
        pub_id (str, optional): Scopus publication ID, to be added to each \
            author dict.
        au_id (str, optional): Scopus author ID, to be added to each author \
            dict.

    Returns:
        dict: A list of dictionaries descrbing each coauthor.
    """
    def parse_author(aut: dict, pub_id: str | None) -> dict:
        filter_dict = {
            'ce:given-name': 'given_name',
            'ce:surname': 'surname',
            '@seq': 'author_order',
            'affiliation': 'affiliation',
            '@auid': 'coauthor_scopus_id'
        }

        filtered = {val: aut.get(key, None)
                    for key, val in filter_dict.items()}

        if (filtered['affiliation']
                and not isinstance(filtered['affiliation'], list)):
            filtered['affiliation'] = [filtered['affiliation']]
        # consolidate affiliations
        filtered["affiliation_ids"] = ','.join(map(
            lambda x: x.get('@id', ""),
            filtered['affiliation']
        )) if filtered['affiliation'] else ''

        del filtered['affiliation']
        if pub_id:
            filtered['publication_id'] = pub_id

        if au_id:
            filtered['author_scid'] = au_id
        return filtered

    req = requests.get(url=url)

    if not req:
        raise RuntimeError("Connection failed. Status: " +
                           str(req.status_code) + ". Reason: " + req.reason)

    content = req.json()
    assert isinstance(content, dict)

    author_list = content['abstracts-retrieval-response']['authors']['author']

    return [parse_author(author, pub_id) for author in author_list]


def get_collaborator_data(
        scopus_id: str | None = None,
        people: list[dict[str, str]] | None = None) -> pd.DataFrame:
    """Construct a dataframe of the collaborator data.

    Args:
        scopus_id (str | None, optional): A Scopus author ID on which to \
            construct the collaborator data. Defaults to None.
        people (list[dict[str, str]] | None, optional): \
            A list of dictionaries, containing at least one element with the \
            field 'Scopus ID'. Defaults to None.

    Returns:
        pd.DataFrame: A dataframe of collaborators.
    """
    # input validation and handling
    if not scopus_id and not people:
        raise ValueError(
            "Must have either `scopus_id` or `people` parameter!"
        )
    elif not scopus_id:
        no_id = list(filter(lambda x: x.get("Scopus ID") is None, people))
        if len(no_id) == len(people):
            raise ValueError(
                "None of the dicts in `people` contain the field 'Scopus ID'."
            )
        if no_id:
            names = list(map(lambda x: x.get("Name"), no_id))
            warn(f"{names} have no field 'Scopus ID'")

        people = [p for p in people if p not in no_id]
    elif not people:
        people = [{"Scopus ID": scopus_id}]
    else:
        raise ValueError(
            "Input either `scopus_id` or `people`, not both."
        )

    sc_params = PARAMS.copy()
    sc_params["count"] = "200"

    pubs = []
    collaborators = []

    for person in people:

        person["author_scid"] = (scopus_id := person.pop("Scopus ID"))
        this_pubs = get_publications(
            scopus_id, params=sc_params, extra_fields=person)

        # add the extra fields in person to the data
        if person:
            for pub in this_pubs:
                pub.update(person)

        pubs.extend(this_pubs)

        # flatten the list of lists with chain and extend collaborators
        # NOTE: This can take a while (~ 2 mins for 200 publicaitons).
        collaborators.extend(chain.from_iterable(
            # get authors for each publication
            get_authors(pub["authors_link"], pub["publication_id"], scopus_id)
            for pub in this_pubs
        ))

    data = (pd.DataFrame(pubs).drop(columns=['authors_link'])
            .merge(
                pd.DataFrame(collaborators),
                on=["publication_id", "author_scid"],
                suffixes=(None, "_collaborator"))
            )

    return data


# %% Run scopus query
if __name__ == "__main__":
    # takes ~12 mins to run.
    collaborator_data = get_collaborator_data(people=research_leaders)
    collaborator_data.to_csv("data/collaborator_data.csv")

# %%
