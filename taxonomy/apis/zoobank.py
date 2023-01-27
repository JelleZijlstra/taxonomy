from dataclasses import dataclass
import json
import pprint
import requests
from typing import Any

from ..db.url_cache import cached, CacheDomain


def clean_lsid(lsid: str) -> str:
    if lsid.startswith("urn:"):
        *_, lsid = lsid.split(":")
    return lsid.upper()


@cached(CacheDomain.zoobank_act)
def _get_zoobank_act_data(query: str) -> str:
    url = f"https://zoobank.org/NomenclaturalActs.json/{query}"
    response = requests.get(url)
    if response.status_code == 404:
        return "[]"
    response.raise_for_status()
    return response.text


@cached(CacheDomain.zoobank_publication)
def _get_zoobank_publication_data(query: str) -> str:
    url = f"https://zoobank.org/References.json/{query}"
    response = requests.get(url)
    if response.status_code == 404:
        return "[]"
    response.raise_for_status()
    return response.text


@dataclass(frozen=True)
class ZooBankData:
    name_lsid: str
    citation_lsid: str


def get_zoobank_data(original_name: str) -> ZooBankData | None:
    api_response = json.loads(_get_zoobank_act_data(original_name.replace(" ", "_")))
    api_response = [
        entry
        for entry in api_response
        if entry["tnuuuid"] == entry["protonymuuid"]
        and entry["namestring"] == original_name.split()[-1]
    ]
    if not api_response:
        return None
    if len(api_response) > 1:
        pprint.pprint(api_response)
        print(f"found multiple ZooBank entries for {original_name}")
        return None
    (data,) = api_response
    ref_uuid = clean_lsid(data["OriginalReferenceUUID"])
    name_uuid = clean_lsid(data["protonymuuid"])
    return ZooBankData(name_uuid, ref_uuid)


def get_zoobank_data_for_article(lsid: str) -> dict[str, Any]:
    ref_data = json.loads(_get_zoobank_publication_data(clean_lsid(lsid)))
    if len(ref_data) != 1:
        raise ValueError(f"unexpected data for reference {lsid}: {ref_data}")
    return ref_data
