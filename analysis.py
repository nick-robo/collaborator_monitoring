"""Analyse the data."""
# %%
import pandas as pd
# import janitor  # noqa: F401
from siuba import (
    group_by, select, _, arrange, head, distinct, join, mutate, if_else,
    filter as filter_tidy, summarize, count, ungroup, rename
)
from siuba.dply.vector import n_distinct, row_number


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
            'Publication Date': pd.to_datetime(df['date']),
            "Citations": pd.to_numeric(df['citedby-count']),
            'Journal': df['journal_name'],
            "Coauthor Name": df['coauth_full'],
            'Coauthor Scopus ID': df['coauthor_scopus_id'],
            'Authorship Order': pd.to_numeric(df['author_order']),
            'Coauthor Affiliation IDs': df['affiliation_ids']
        }))
    )

# %%


path = 'data/collaborator_data.csv'
data = clean_data(path)

weird_names = (
    data >>
    group_by(
        _['Coauthor Scopus ID'],
    ) >>
    filter_tidy(
        n_distinct(_['Coauthor Name']) > 1
    ) >>
    ungroup() >>
    select(_['Coauthor Scopus ID'], _['Coauthor Name']) >>
    group_by(_['Coauthor Scopus ID'], _['Coauthor Name']) >>
    summarize(
        n=_.shape[0]
    ) >>
    filter_tidy(
        ~_['Coauthor Name'].isna()
    ) >>
    group_by(_['Coauthor Scopus ID']) >>
    # choose the most common name in the dataset.
    filter_tidy(
        _['n'] == _['n'].max()
    ) >>
    # choose the longest out of the ramining names (if most common tied)
    filter_tidy(
        _['Coauthor Name'].str.len() == _['Coauthor Name'].str.len().max()
    ) >>
    filter_tidy(row_number(_) == 1) >>
    ungroup() >>
    rename(CoAuth_Name=_['Coauthor Name'])
)

check_unique = (
    weird_names >>
    group_by(_['Coauthor Scopus ID']) >>
    count() >> filter_tidy(_.n != 1) >>
    join(
        _,
        weird_names >> select(_['Coauthor Scopus ID'], _['CoAuth_Name']),
        how='inner'
    )
)

assert not (n := len(check_unique["Coauthor Scopus ID"].unique())), (
    f"{str(n) + ' IDs are' if n > 1 else ' ID is'}" +
    "assicciated with more that one name. "
)


# replace divergent name with chosen ones

data = (
    data >>
    join(
        _,
        weird_names >> select(_['Coauthor Scopus ID'], _['CoAuth_Name']),
        how='left'
    ) >>
    mutate(
        chosen_name=if_else(
            _['CoAuth_Name'].isna(),
            _['Coauthor Name'],
            _['CoAuth_Name']
        )
    )
)

data['Coauthor Name'] = data.chosen_name

# drop working columns
data: pd.DataFrame = (
    data >>
    select(~_['CoAuth_Name'], ~_.chosen_name, ~_.n)
)

del check_unique, weird_names

# %%

common_collabs: pd.DataFrame = (
    data >>
    filter_tidy(
        ~_['Coauthor Scopus ID'].isin(_['Author Scopus ID'].unique()),
        _["Publication Subtype"] == 'Article'
    ) >>
    group_by(
        # _['Coauthor Name'],
        _['Coauthor Scopus ID'],
        # _['Coauthor Affiliation IDs']
    ) >>
    arrange(
        -_['Publication Date'])
    >>
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

common_collabs_aID: pd.DataFrame = (
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
