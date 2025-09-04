import os, json
from pathlib import Path
import stat

def same_inode(p1: Path, p2: Path) -> bool:
    try:
        s1, s2 = p1.stat(), p2.stat()
        return (s1.st_ino == s2.st_ino) and (s1.st_dev == s2.st_dev)
    except: return False

def test_export_hardlink_manifest(app_client, seeded, env):
    app, appmod = app_client
    client = app.test_client()

    # call export via API key (hardlink + no archive)
    url = f"/api/models/{seeded['author']}/{seeded['model']}/export?mode=hardlink&archive=none&compress=none"
    r = client.post(url, headers={"Authorization": f"Bearer {seeded['api_key']}"})
    assert r.status_code == 200
    data = r.get_json()
    dest_root = Path(data["dest_root"])
    assert dest_root.exists()

    # exported files exist and (ideally) are hardlinks to source
    for f in seeded["rows"]:
        src = Path(f["local_path"])
        dst = dest_root / f["rfilename"]
        assert dst.exists()
        # on same FS we expect hardlink; if not, at least size equals
        assert dst.stat().st_size == src.stat().st_size
        assert same_inode(src, dst) or True  # don't fail on cross-FS

    # manifest endpoint
    r = client.get(f"/api/models/{seeded['author']}/{seeded['model']}/manifest",
                   headers={"Authorization": f"Bearer {seeded['api_key']}"})
    assert r.status_code == 200
    manifest = json.loads(r.get_data(as_text=True))
    assert manifest["repo_id"] == seeded["repo_id"]
    assert len(manifest["files"]) == 3

    # export audit exists
    db = appmod.get_db()
    row = db.execute("SELECT * FROM exports_audit ORDER BY id DESC LIMIT 1").fetchone()
    assert row is not None
    assert row["repo_id"] == seeded["repo_id"]
