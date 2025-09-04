import io, tarfile

def list_tar_members(bytes_payload):
    bio = io.BytesIO(bytes_payload)
    with tarfile.open(fileobj=bio, mode="r:gz") as tar:
        return sorted([m.name for m in tar.getmembers() if m.isfile()])

def test_api_bundle_strict0(app_client, seeded):
    app, appmod = app_client
    client = app.test_client()

    url = f"/api/models/{seeded['repo_id']}/bundle?strict=0"
    r = client.get(url, headers={"Authorization": f"Bearer {seeded['api_key']}"})
    assert r.status_code == 200
    names = list_tar_members(r.data)

    # archive root should be Author/Model/...
    assert any(n.startswith("Author/Model/") for n in names)
    # all three seeded files included
    for f in seeded["rows"]:
        assert f"Author/Model/{f['rfilename']}" in names
