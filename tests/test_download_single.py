def test_api_single_download(app_client, seeded):
    app, appmod = app_client
    client = app.test_client()
    rf = seeded["rows"][0]["rfilename"]
    # use url_for to avoid path converter ambiguity
    with app.test_request_context():
        url = app.url_for("api_download", repo_id=seeded["repo_id"], rfilename=rf)

    r = client.get(url, headers={"Authorization": f"Bearer {seeded['api_key']}"})
    assert r.status_code == 200
    assert r.data  # got file bytes

    # audit row exists
    db = appmod.get_db()
    row = db.execute("SELECT outcome, via FROM downloads_audit ORDER BY id DESC LIMIT 1").fetchone()
    assert row["outcome"] == "ALLOW" and "api_key" in row["via"]

def test_ui_single_download(app_client, seeded):
    app, appmod = app_client
    client = app.test_client()
    # login
    client.post("/login", data={"email": seeded["user_email"], "password": seeded["user_pw"]}, follow_redirects=True)

    rf = seeded["rows"][1]["rfilename"]
    r = client.get(f"/download/{seeded['repo_id']}/{rf}")
    assert r.status_code == 200
    assert r.data
