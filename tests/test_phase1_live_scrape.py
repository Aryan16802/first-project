from pathlib import Path

from mf_rag.ingestion.groww_client import GrowwClient, SELECTED_GROWW_FUND_URLS


def test_extract_next_data_and_parse_live_records(tmp_path: Path) -> None:
    sample = tmp_path / "sample.json"
    sample.write_text('{"schemes": []}', encoding="utf-8")
    client = GrowwClient(sample_file=sample, use_live=True)

    html = """
    <html><head></head><body>
    <script id="__NEXT_DATA__" type="application/json">
    {"props":{"pageProps":{"items":[
      {"schemeId":"INF0001","fundName":"Axis ELSS Tax Saver Fund","fundHouse":"Axis Mutual Fund","category":"Equity","subCategory":"ELSS"},
      {"schemeId":"INF0002","fundName":"Parag Parikh Flexi Cap","fundHouse":"PPFAS Mutual Fund","category":"Equity","subCategory":"Flexi Cap"}
    ]}}}
    </script>
    </body></html>
    """
    data = client._extract_next_data_json(html)
    assert data is not None

    found = []
    for node in client._iter_dicts(data):
        if "schemeId" in node and "fundName" in node:
            found.append(client._normalize_live_scheme(node))
    assert len(found) == 2
    assert found[0]["scheme_name"] == "Axis ELSS Tax Saver Fund"
    assert found[0]["scheme_id"] == "INF0001"


def test_live_fetch_falls_back_to_sample_on_failure(tmp_path: Path) -> None:
    sample = tmp_path / "sample.json"
    sample.write_text(
        '{"schemes":[{"scheme_id":"INF_SAMPLE","scheme_name":"Sample Fund","amc_name":"AMC","category":"Debt"}]}',
        encoding="utf-8",
    )
    client = GrowwClient(sample_file=sample, use_live=True)

    # Simulate live failure by monkeypatching method.
    client._fetch_live_selected = lambda: []  # type: ignore[method-assign]
    client._fetch_live = lambda: []  # type: ignore[method-assign]
    schemes = client.fetch_scheme_master()
    assert len(schemes) == 1
    assert schemes[0]["scheme_id"] == "INF_SAMPLE"


def test_selected_live_urls_are_exactly_ten() -> None:
    assert len(SELECTED_GROWW_FUND_URLS) == 10
