"""Analyse the data."""
# %%
import pandas as pd
# import janitor  # noqa: F401
from siuba import (
    group_by, select, _, arrange, head, distinct, join,
    filter as filter_tidy, summarize, count
)


def clean_data(path: str) -> pd.DataFrame:
    """Clean the data.

    Args:
        path (str): Path to the raw data.

    Returns:
        pd.DataFrame: The cleaned data.
    """
    data = pd.read_csv(path)

    junk_cols = filter(lambda x: "Unnamed" in x, data.columns)

    data = (
        data.drop(columns=junk_cols)
    )

    return (
        data.assign(coauth_full=data.given_name + " " + data.surname)
        .drop(columns=["given_name", "surname"])
        .pipe(lambda df: pd.DataFrame({
            'Author Name': df['Name'],
            'Author Scopus ID': df['author_scid'],
            'Author ORCID': df['ORCID'],
            'Publication ID': df['publication_id'],
            'DOI': df['doi'],
            'Publication Type': df['type'],
            'Publication Subtype': df['subtype'],
            'Publication Date': df['date'],
            "Citations": df['citedby-count'],
            'Journal': df['journal_name'],
            "Coauthor Name": df['coauth_full'],
            'Coauthor Scopus ID': df['coauthor_scopus_id'],
            'Authorship Order': df['author_order'],
            'Coauthor Affiliation IDs': df['affiliation_ids']
        }))
    )

# %%


path = 'data/collaborator_data.csv'
data = clean_data(path)

# %%

common_collabs = (
    data >>
    filter_tidy(
        ~_['Coauthor Scopus ID'].isin(_['Author Scopus ID'].unique())
    ) >>
    group_by(
        _['Coauthor Name'],
        _['Coauthor Scopus ID'],
        # _['Coauthor Affiliation IDs']
    ) >>
    summarize(
        n=_.shape[0]
    ) >>
    arrange(-_.n)
)

# %% get all coauthors with each aid

aID_temp: pd.DataFrame = (
    data >>
    select(_['Coauthor Scopus ID'], _['Coauthor Affiliation IDs']) >>
    distinct()
)
aID_temp["Coauthor Affiliation IDs"] = (
    aID_temp["Coauthor Affiliation IDs"]
    .str.split(','))
aID_temp = aID_temp.explode("Coauthor Affiliation IDs") >> distinct()

common_collabs_aID = (
    common_collabs >>
    join(
        _,
        aID_temp,
        how='inner'
    )
)

del aID_temp

common_collabs_aID >> head()

# number of unique affiliations
print(
    common_collabs_aID >>
    select(_["Coauthor Affiliation IDs"]) >>
    distinct() >>
    count()
)
# %%
