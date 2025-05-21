import json
import os
from datetime import datetime

import requests

import lxml.etree as et
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = "https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom"
MAX_PAGES = 5

state = json.load(open("state.json")) if os.path.exists("state.json") else {}
last_ingested = datetime.fromisoformat(state.get("last_updated_ingested", "1970-01-01T00:00:00+00:00"))

rows, n_page, completed = [], 0, False
url = ROOT
while url and not completed and n_page < MAX_PAGES:
    n_page += 1
    response = requests.get(url, timeout=30)
    feed = et.fromstring(response.content)
    ns = feed.nsmap

    entries = feed.findall("entry", namespaces=ns)
    if not entries:
        break

    for entry in entries:
        entry_id = entry.findtext("id", namespaces=ns)
        updated = datetime.fromisoformat(entry.findtext("updated", namespaces=ns))
        if updated <= last_ingested:
            completed = True
            break

        blob = et.tostring(entry, encoding="unicode")

        rows.append({
            "id": entry_id,
            "updated": updated,
            "folder_id": entry.findtext(".//cbc:ContractFolderID", default="", namespaces=ns),
            "status": entry.findtext(".//cac-place-ext:ContractFolderStatus/"
                                     "cbc-place-ext:ContractFolderStatusCode", default="", namespaces=ns),
            "cpv": entry.findtext(".//cac:RequiredCommodityClassification/"
                                  "cbc:ItemClassificationCode", default="", namespaces=ns),
            "budget_value": float(entry.findtext(".//cbc:TotalAmount", default=0, namespaces=ns)),
            "estimated_value": float(entry.findtext(".//cbc:EstimatedOverallContractAmount",
                                                    default=0, namespaces=ns)),
            "award_value": float(entry.findtext(".//cac:LegalMonetaryTotal/"
                                                "cbc:TaxExclusiveAmount", default=0, namespaces=ns)),
            "nuts": entry.findtext(".//cac:RealizedLocation/"
                                   "cbc:CountrySubentityCode", default="", namespaces=ns),
            "award_date": entry.findtext(".//cac:TenderResult/"
                                         "cbc:AwardDate", default="", namespaces=ns),
            "raw_xml": blob,
        })

    link_next = feed.find("link[@rel='next']", ns)
    url = link_next.get("href") if link_next is not None else None

schema = pa.schema([
    ("id", pa.string()),
    ("updated",         pa.timestamp("ms", tz="Europe/Madrid")),
    ("folder_id",       pa.string()),
    ("status",          pa.string()),
    ("cpv",             pa.string()),
    ("budget_value",    pa.float64()),
    ("estimated_value", pa.float64()),
    ("award_value",     pa.float64()),
    ("nuts",            pa.string()),
    ("award_date",      pa.string()),
    ("raw_xml",         pa.string()),
])

if rows:
    table = pa.Table.from_pylist(mapping=rows, schema=schema)
    pq.write_to_dataset(table, root_path="data/parquet")
    state["last_updated_ingested"] = max(r["updated"] for r in rows).isoformat()
    json.dump(state, open("state.json", "w"))
    print(f"Ingested {len(rows)} new entries")
else:
    print("Nothing new")
